import os
import json
import asyncio
from typing import List, Tuple, Dict
from functools import partial
from uuid import uuid4
from datetime import datetime, timezone
from base64 import b64encode

import logging
from typing import Optional, AsyncGenerator
from urllib.parse import unquote
from fastapi import HTTPException, Request, status
from fastapi.background import BackgroundTasks
from fastapi.responses import JSONResponse
from redis import asyncio as aioredis

from crawl4ai import (
    AsyncWebCrawler,
    CrawlerRunConfig,
    LLMExtractionStrategy,
    CacheMode,
    BrowserConfig,
    MemoryAdaptiveDispatcher,
    RateLimiter, 
    LLMConfig
)
from crawl4ai.utils import perform_completion_with_backoff
from crawl4ai.content_filter_strategy import (
    PruningContentFilter,
    BM25ContentFilter,
    LLMContentFilter
)
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy

from utils import (
    TaskStatus,
    FilterType,
    get_base_url,
    is_task_id,
    should_cleanup_task,
    decode_redis_hash,
    get_llm_api_key,
    validate_llm_provider,
    get_llm_temperature,
    get_llm_base_url,
    load_config
)
from webhook import WebhookDeliveryService
from token_manager import (
    extract_domain_from_url,
    load_token_for_domain,
    generate_hooks_code,
    get_crawler_config_overrides,
)

import psutil, time

logger = logging.getLogger(__name__)

# --- Helper to get memory ---
def _get_memory_mb():
    try:
        return psutil.Process().memory_info().rss / (1024 * 1024)
    except Exception as e:
        logger.warning(f"Could not get memory info: {e}")
        return None


async def handle_llm_qa(
    url: str,
    query: str,
    config: dict
) -> str:
    """Process QA using LLM with crawled content as context."""
    try:
        if not url.startswith(('http://', 'https://')) and not url.startswith(("raw:", "raw://")):
            url = 'https://' + url
        # Extract base URL by finding last '?q=' occurrence
        last_q_index = url.rfind('?q=')
        if last_q_index != -1:
            url = url[:last_q_index]

        # Get markdown content
        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(url)
            if not result.success:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=result.error_message
                )
            content = result.markdown.fit_markdown or result.markdown.raw_markdown

        # Create prompt and get LLM response
        prompt = f"""Use the following content as context to answer the question.
    Content:
    {content}

    Question: {query}

    Answer:"""

        # api_token=os.environ.get(config["llm"].get("api_key_env", ""))

        response = perform_completion_with_backoff(
            provider=config["llm"]["provider"],
            prompt_with_variables=prompt,
            api_token=get_llm_api_key(config),  # Returns None to let litellm handle it
            temperature=get_llm_temperature(config),
            base_url=get_llm_base_url(config)
        )
        
                # response = perform_completion_with_backoff(
        #     provider='openai/qwen_14b',
        #     prompt_with_variables=prompt,
        #     api_token='32000', # Returns None to let litellm handle it
        #     temperature=0.0,
        #     base_url='http://192.168.129.95:8800/v1'
        # )

        return response.choices[0].message.content
    except Exception as e:
        logger.error(f"QA processing error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

async def process_llm_extraction(
    redis: aioredis.Redis,
    config: dict,
    task_id: str,
    url: str,
    instruction: str,
    schema: Optional[str] = None,
    cache: str = "0",
    provider: Optional[str] = None,
    webhook_config: Optional[Dict] = None,
    temperature: Optional[float] = None,
    base_url: Optional[str] = None
) -> None:
    """Process LLM extraction in background."""
    # Initialize webhook service
    webhook_service = WebhookDeliveryService(config)

    try:
        # Validate provider
        is_valid, error_msg = validate_llm_provider(config, provider)
        if not is_valid:
            await redis.hset(f"task:{task_id}", mapping={
                "status": TaskStatus.FAILED,
                "error": error_msg
            })

            # Send webhook notification on failure
            await webhook_service.notify_job_completion(
                task_id=task_id,
                task_type="llm_extraction",
                status="failed",
                urls=[url],
                webhook_config=webhook_config,
                error=error_msg
            )
            return
        api_key = get_llm_api_key(config, provider)  # Returns None to let litellm handle it
        llm_strategy = LLMExtractionStrategy(
            llm_config=LLMConfig(
                provider=provider or config["llm"]["provider"],
                api_token=api_key,
                temperature=temperature or get_llm_temperature(config, provider),
                base_url=base_url or get_llm_base_url(config, provider)
            ),
            instruction=instruction,
            schema=json.loads(schema) if schema else None,
        )

        cache_mode = CacheMode.ENABLED if cache == "1" else CacheMode.WRITE_ONLY

        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(
                url=url,
                config=CrawlerRunConfig(
                    extraction_strategy=llm_strategy,
                    scraping_strategy=LXMLWebScrapingStrategy(),
                    cache_mode=cache_mode
                )
            )

        if not result.success:
            await redis.hset(f"task:{task_id}", mapping={
                "status": TaskStatus.FAILED,
                "error": result.error_message
            })

            # Send webhook notification on failure
            await webhook_service.notify_job_completion(
                task_id=task_id,
                task_type="llm_extraction",
                status="failed",
                urls=[url],
                webhook_config=webhook_config,
                error=result.error_message
            )
            return

        try:
            content = json.loads(result.extracted_content)
        except json.JSONDecodeError:
            content = result.extracted_content

        result_data = {"extracted_content": content}

        await redis.hset(f"task:{task_id}", mapping={
            "status": TaskStatus.COMPLETED,
            "result": json.dumps(content)
        })

        # Send webhook notification on successful completion
        await webhook_service.notify_job_completion(
            task_id=task_id,
            task_type="llm_extraction",
            status="completed",
            urls=[url],
            webhook_config=webhook_config,
            result=result_data
        )

    except Exception as e:
        logger.error(f"LLM extraction error: {str(e)}", exc_info=True)
        await redis.hset(f"task:{task_id}", mapping={
            "status": TaskStatus.FAILED,
            "error": str(e)
        })

        # Send webhook notification on failure
        await webhook_service.notify_job_completion(
            task_id=task_id,
            task_type="llm_extraction",
            status="failed",
            urls=[url],
            webhook_config=webhook_config,
            error=str(e)
        )

async def handle_markdown_request(
    url: str,
    filter_type: FilterType,
    query: Optional[str] = None,
    cache: str = "0",
    config: Optional[dict] = None,
    provider: Optional[str] = None,
    temperature: Optional[float] = None,
    base_url: Optional[str] = None
) -> str:
    """Handle markdown generation requests."""
    try:
        # Validate provider if using LLM filter
        if filter_type == FilterType.LLM:
            is_valid, error_msg = validate_llm_provider(config, provider)
            if not is_valid:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=error_msg
                )
        decoded_url = unquote(url)
        if not decoded_url.startswith(('http://', 'https://')) and not decoded_url.startswith(("raw:", "raw://")):
            decoded_url = 'https://' + decoded_url

        if filter_type == FilterType.RAW:
            md_generator = DefaultMarkdownGenerator()
        else:
            content_filter = {
                FilterType.FIT: PruningContentFilter(),
                FilterType.BM25: BM25ContentFilter(user_query=query or ""),
                FilterType.LLM: LLMContentFilter(
                    llm_config=LLMConfig(
                        provider=provider or config["llm"]["provider"],
                        api_token=get_llm_api_key(config, provider),  # Returns None to let litellm handle it
                        temperature=temperature or get_llm_temperature(config, provider),
                        base_url=base_url or get_llm_base_url(config, provider)
                    ),
                    instruction=query or "Extract main content"
                )
            }[filter_type]
            md_generator = DefaultMarkdownGenerator(content_filter=content_filter)

        cache_mode = CacheMode.ENABLED if cache == "1" else CacheMode.WRITE_ONLY
        
                # extraction_strategy = LLMExtractionStrategy(
        #     llm_config=LLMConfig(
        #                 provider=provider or config["llm"]["provider"],
        #                 api_token=get_llm_api_key(
        #                     config, provider
        #                 ),  # Returns None to let litellm handle it
        #                 temperature=temperature
        #                 or get_llm_temperature(config, provider),
        #                 base_url=base_url or get_llm_base_url(config, provider),
        #             ),
        #     schema=None,
        #     extraction_type="json",
        #     instruction="""
        #     这是一个搜索引擎网页返回的界面，整理搜索结果，并将结果按照以下格式输出:["title:网页标题,url:网页链接","title:网页标题,url:网页链接"]
        #     """,
        #     chunk_token_threshold=4096,
        #     overlap_rate=0.001,
        #     apply_chunking=True,
        #     input_format="html",
        # )

        # # Build crawler config
        # crawl_config = CrawlerRunConfig(
        #     extraction_strategy=extraction_strategy,
        #     cache_mode=CacheMode.BYPASS
        # )

        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(
                url=decoded_url,
                config=CrawlerRunConfig(
                    markdown_generator=md_generator,
                    scraping_strategy=LXMLWebScrapingStrategy(),
                    cache_mode=cache_mode
                )
            )
            
            if not result.success:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=result.error_message
                )

            return (result.markdown.raw_markdown 
                   if filter_type == FilterType.RAW 
                   else result.markdown.fit_markdown)

    except Exception as e:
        logger.error(f"Markdown error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

async def handle_llm_request(
    redis: aioredis.Redis,
    background_tasks: BackgroundTasks,
    request: Request,
    input_path: str,
    query: Optional[str] = None,
    schema: Optional[str] = None,
    cache: str = "0",
    config: Optional[dict] = None,
    provider: Optional[str] = None,
    webhook_config: Optional[Dict] = None,
    temperature: Optional[float] = None,
    api_base_url: Optional[str] = None
) -> JSONResponse:
    """Handle LLM extraction requests."""
    base_url = get_base_url(request)
    
    try:
        if is_task_id(input_path):
            return await handle_task_status(
                redis, input_path, base_url
            )

        if not query:
            return JSONResponse({
                "message": "Please provide an instruction",
                "_links": {
                    "example": {
                        "href": f"{base_url}/llm/{input_path}?q=Extract+main+content",
                        "title": "Try this example"
                    }
                }
            })

        return await create_new_task(
            redis,
            background_tasks,
            input_path,
            query,
            schema,
            cache,
            base_url,
            config,
            provider,
            webhook_config,
            temperature,
            api_base_url
        )

    except Exception as e:
        logger.error(f"LLM endpoint error: {str(e)}", exc_info=True)
        return JSONResponse({
            "error": str(e),
            "_links": {
                "retry": {"href": str(request.url)}
            }
        }, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

async def handle_task_status(
    redis: aioredis.Redis,
    task_id: str,
    base_url: str,
    *,
    keep: bool = False
) -> JSONResponse:
    """Handle task status check requests."""
    task = await redis.hgetall(f"task:{task_id}")
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found"
        )

    task = decode_redis_hash(task)
    response = create_task_response(task, task_id, base_url)

    if task["status"] in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
        if not keep and should_cleanup_task(task["created_at"]):
            await redis.delete(f"task:{task_id}")

    return JSONResponse(response)

async def create_new_task(
    redis: aioredis.Redis,
    background_tasks: BackgroundTasks,
    input_path: str,
    query: str,
    schema: Optional[str],
    cache: str,
    base_url: str,
    config: dict,
    provider: Optional[str] = None,
    webhook_config: Optional[Dict] = None,
    temperature: Optional[float] = None,
    api_base_url: Optional[str] = None
) -> JSONResponse:
    """Create and initialize a new task."""
    decoded_url = unquote(input_path)
    if not decoded_url.startswith(('http://', 'https://')) and not decoded_url.startswith(("raw:", "raw://")):
        decoded_url = 'https://' + decoded_url

    from datetime import datetime
    task_id = f"llm_{int(datetime.now().timestamp())}_{id(background_tasks)}"

    task_data = {
        "status": TaskStatus.PROCESSING,
        "created_at": datetime.now().isoformat(),
        "url": decoded_url
    }

    # Store webhook config if provided
    if webhook_config:
        task_data["webhook_config"] = json.dumps(webhook_config)

    await redis.hset(f"task:{task_id}", mapping=task_data)

    background_tasks.add_task(
        process_llm_extraction,
        redis,
        config,
        task_id,
        decoded_url,
        query,
        schema,
        cache,
        provider,
        webhook_config,
        temperature,
        api_base_url
    )

    return JSONResponse({
        "task_id": task_id,
        "status": TaskStatus.PROCESSING,
        "url": decoded_url,
        "_links": {
            "self": {"href": f"{base_url}/llm/{task_id}"},
            "status": {"href": f"{base_url}/llm/{task_id}"}
        }
    })

def create_task_response(task: dict, task_id: str, base_url: str) -> dict:
    """Create response for task status check."""
    response = {
        "task_id": task_id,
        "status": task["status"],
        "created_at": task["created_at"],
        "url": task["url"],
        "_links": {
            "self": {"href": f"{base_url}/llm/{task_id}"},
            "refresh": {"href": f"{base_url}/llm/{task_id}"}
        }
    }

    if task["status"] == TaskStatus.COMPLETED:
        response["result"] = json.loads(task["result"])
    elif task["status"] == TaskStatus.FAILED:
        response["error"] = task["error"]

    return response

async def stream_results(crawler: AsyncWebCrawler, results_gen: AsyncGenerator) -> AsyncGenerator[bytes, None]:
    """Stream results with heartbeats and completion markers."""
    import json
    from utils import datetime_handler

    try:
        async for result in results_gen:
            try:
                server_memory_mb = _get_memory_mb()
                result_dict = result.model_dump()
                result_dict['server_memory_mb'] = server_memory_mb
                # Ensure fit_html is JSON-serializable
                if "fit_html" in result_dict and not (result_dict["fit_html"] is None or isinstance(result_dict["fit_html"], str)):
                    result_dict["fit_html"] = None
                # If PDF exists, encode it to base64
                if result_dict.get('pdf') is not None:
                    result_dict['pdf'] = b64encode(result_dict['pdf']).decode('utf-8')
                logger.info(f"Streaming result for {result_dict.get('url', 'unknown')}")
                data = json.dumps(result_dict, default=datetime_handler) + "\n"
                yield data.encode('utf-8')
            except Exception as e:
                logger.error(f"Serialization error: {e}")
                error_response = {"error": str(e), "url": getattr(result, 'url', 'unknown')}
                yield (json.dumps(error_response) + "\n").encode('utf-8')

        yield json.dumps({"status": "completed"}).encode('utf-8')
        
    except asyncio.CancelledError:
        logger.warning("Client disconnected during streaming")
    finally:
        # try:
        #     await crawler.close()
        # except Exception as e:
        #     logger.error(f"Crawler cleanup error: {e}")
        pass

async def handle_crawl_request(
    urls: List[str],
    browser_config: dict,
    crawler_config: dict,
    config: dict,
    hooks_config: Optional[dict] = None
) -> dict:
    """Handle non-streaming crawl requests with optional hooks."""
    start_mem_mb = _get_memory_mb() # <--- Get memory before
    start_time = time.time()
    mem_delta_mb = None
    peak_mem_mb = start_mem_mb
    hook_manager = None
    
    try:
        urls = [('https://' + url) if not url.startswith(('http://', 'https://')) and not url.startswith(("raw:", "raw://")) else url for url in urls]
        browser_config = BrowserConfig.load(browser_config)
        crawler_config = CrawlerRunConfig.load(crawler_config)

        dispatcher = MemoryAdaptiveDispatcher(
            memory_threshold_percent=config["crawler"]["memory_threshold_percent"],
            rate_limiter=RateLimiter(
                base_delay=tuple(config["crawler"]["rate_limiter"]["base_delay"])
            ) if config["crawler"]["rate_limiter"]["enabled"] else None
        )
        
        from crawler_pool import get_crawler
        crawler = await get_crawler(browser_config)

        # crawler: AsyncWebCrawler = AsyncWebCrawler(config=browser_config)
        # await crawler.start()
        
        # Attach hooks if provided
        hooks_status = {}
        if hooks_config:
            from hook_manager import attach_user_hooks_to_crawler, UserHookManager
            hook_manager = UserHookManager(timeout=hooks_config.get('timeout', 30))
            hooks_status, hook_manager = await attach_user_hooks_to_crawler(
                crawler,
                hooks_config.get('code', {}),
                timeout=hooks_config.get('timeout', 30),
                hook_manager=hook_manager
            )
            logger.info(f"Hooks attachment status: {hooks_status['status']}")
        
        base_config = config["crawler"]["base_config"]
        # Iterate on key-value pairs in global_config then use hasattr to set them
        for key, value in base_config.items():
            if hasattr(crawler_config, key):
                current_value = getattr(crawler_config, key)
                # Only set base config if user didn't provide a value
                if current_value is None or current_value == "":
                    setattr(crawler_config, key, value)

        results = []
        func = getattr(crawler, "arun" if len(urls) == 1 else "arun_many")
        partial_func = partial(func, 
                                urls[0] if len(urls) == 1 else urls, 
                                config=crawler_config, 
                                dispatcher=dispatcher)
        results = await partial_func()
        
        # Ensure results is always a list
        if not isinstance(results, list):
            results = [results]

        # await crawler.close()
        
        end_mem_mb = _get_memory_mb() # <--- Get memory after
        end_time = time.time()
        
        if start_mem_mb is not None and end_mem_mb is not None:
            mem_delta_mb = end_mem_mb - start_mem_mb # <--- Calculate delta
            peak_mem_mb = max(peak_mem_mb if peak_mem_mb else 0, end_mem_mb) # <--- Get peak memory
        logger.info(f"Memory usage: Start: {start_mem_mb} MB, End: {end_mem_mb} MB, Delta: {mem_delta_mb} MB, Peak: {peak_mem_mb} MB")

        # Process results to handle PDF bytes
        processed_results = []
        for result in results:
            try:
                # Check if result has model_dump method (is a proper CrawlResult)
                if hasattr(result, 'model_dump'):
                    result_dict = result.model_dump()
                elif isinstance(result, dict):
                    result_dict = result
                else:
                    # Handle unexpected result type
                    logger.warning(f"Unexpected result type: {type(result)}")
                    result_dict = {
                        "url": str(result) if hasattr(result, '__str__') else "unknown",
                        "success": False,
                        "error_message": f"Unexpected result type: {type(result).__name__}"
                    }
                
                # if fit_html is not a string, set it to None to avoid serialization errors
                if "fit_html" in result_dict and not (result_dict["fit_html"] is None or isinstance(result_dict["fit_html"], str)):
                    result_dict["fit_html"] = None
                    
                # If PDF exists, encode it to base64
                if result_dict.get('pdf') is not None and isinstance(result_dict.get('pdf'), bytes):
                    result_dict['pdf'] = b64encode(result_dict['pdf']).decode('utf-8')
                    
                processed_results.append(result_dict)
            except Exception as e:
                logger.error(f"Error processing result: {e}")
                processed_results.append({
                    "url": "unknown",
                    "success": False,
                    "error_message": str(e)
                })
            
        response = {
            "success": True,
            "results": processed_results,
            "server_processing_time_s": end_time - start_time,
            "server_memory_delta_mb": mem_delta_mb,
            "server_peak_memory_mb": peak_mem_mb
        }
        
        # Add hooks information if hooks were used
        if hooks_config and hook_manager:
            from hook_manager import UserHookManager
            if isinstance(hook_manager, UserHookManager):
                try:
                    # Ensure all hook data is JSON serializable
                    hook_data = {
                        "status": hooks_status,
                        "execution_log": hook_manager.execution_log,
                        "errors": hook_manager.errors,
                        "summary": hook_manager.get_summary()
                    }
                    # Test that it's serializable
                    json.dumps(hook_data)
                    response["hooks"] = hook_data
                except (TypeError, ValueError) as e:
                    logger.error(f"Hook data not JSON serializable: {e}")
                    response["hooks"] = {
                        "status": {"status": "error", "message": "Hook data serialization failed"},
                        "execution_log": [],
                        "errors": [{"error": str(e)}],
                        "summary": {}
                    }
        
        return response

    except Exception as e:
        logger.error(f"Crawl error: {str(e)}", exc_info=True)
        if 'crawler' in locals() and crawler.ready: # Check if crawler was initialized and started
            #  try:
            #      await crawler.close()
            #  except Exception as close_e:
            #       logger.error(f"Error closing crawler during exception handling: {close_e}")
            logger.error(f"Error closing crawler during exception handling: {str(e)}")

        # Measure memory even on error if possible
        end_mem_mb_error = _get_memory_mb()
        if start_mem_mb is not None and end_mem_mb_error is not None:
            mem_delta_mb = end_mem_mb_error - start_mem_mb

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=json.dumps({ # Send structured error
                "error": str(e),
                "server_memory_delta_mb": mem_delta_mb,
                "server_peak_memory_mb": max(peak_mem_mb if peak_mem_mb else 0, end_mem_mb_error or 0)
            })
        )

async def handle_stream_crawl_request(
    urls: List[str],
    browser_config: dict,
    crawler_config: dict,
    config: dict,
    hooks_config: Optional[dict] = None
) -> Tuple[AsyncWebCrawler, AsyncGenerator, Optional[Dict]]:
    """Handle streaming crawl requests with optional hooks."""
    hooks_info = None
    try:
        browser_config = BrowserConfig.load(browser_config)
        # browser_config.verbose = True # Set to False or remove for production stress testing
        browser_config.verbose = False
        crawler_config = CrawlerRunConfig.load(crawler_config)
        crawler_config.scraping_strategy = LXMLWebScrapingStrategy()
        crawler_config.stream = True

        dispatcher = MemoryAdaptiveDispatcher(
            memory_threshold_percent=config["crawler"]["memory_threshold_percent"],
            rate_limiter=RateLimiter(
                base_delay=tuple(config["crawler"]["rate_limiter"]["base_delay"])
            )
        )

        from crawler_pool import get_crawler
        crawler = await get_crawler(browser_config)

        # crawler = AsyncWebCrawler(config=browser_config)
        # await crawler.start()
        
        # Attach hooks if provided
        if hooks_config:
            from hook_manager import attach_user_hooks_to_crawler, UserHookManager
            hook_manager = UserHookManager(timeout=hooks_config.get('timeout', 30))
            hooks_status, hook_manager = await attach_user_hooks_to_crawler(
                crawler,
                hooks_config.get('code', {}),
                timeout=hooks_config.get('timeout', 30),
                hook_manager=hook_manager
            )
            logger.info(f"Hooks attachment status for streaming: {hooks_status['status']}")
            # Include hook manager in hooks_info for proper tracking
            hooks_info = {'status': hooks_status, 'manager': hook_manager}

        results_gen = await crawler.arun_many(
            urls=urls,
            config=crawler_config,
            dispatcher=dispatcher
        )

        return crawler, results_gen, hooks_info

    except Exception as e:
        # Make sure to close crawler if started during an error here
        if 'crawler' in locals() and crawler.ready:
            #  try:
            #       await crawler.close()
            #  except Exception as close_e:
            #       logger.error(f"Error closing crawler during stream setup exception: {close_e}")
            logger.error(f"Error closing crawler during stream setup exception: {str(e)}")
        logger.error(f"Stream crawl error: {str(e)}", exc_info=True)
        # Raising HTTPException here will prevent streaming response
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

async def handle_crawl_job(
    redis,
    background_tasks: BackgroundTasks,
    urls: List[str],
    browser_config: Dict,
    crawler_config: Dict,
    config: Dict,
    webhook_config: Optional[Dict] = None,
) -> Dict:
    """
    Fire-and-forget version of handle_crawl_request.
    Creates a task in Redis, runs the heavy work in a background task,
    lets /crawl/job/{task_id} polling fetch the result.
    """
    task_id = f"crawl_{uuid4().hex[:8]}"

    # Store task data in Redis
    task_data = {
        "status": TaskStatus.PROCESSING,         # <-- keep enum values consistent
        "created_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        "url": json.dumps(urls),                 # store list as JSON string
        "result": "",
        "error": "",
    }

    # Store webhook config if provided
    if webhook_config:
        task_data["webhook_config"] = json.dumps(webhook_config)

    await redis.hset(f"task:{task_id}", mapping=task_data)

    # Initialize webhook service
    webhook_service = WebhookDeliveryService(config)

    async def _runner():
        try:
            result = await handle_crawl_request(
                urls=urls,
                browser_config=browser_config,
                crawler_config=crawler_config,
                config=config,
            )
            await redis.hset(f"task:{task_id}", mapping={
                "status": TaskStatus.COMPLETED,
                "result": json.dumps(result),
            })

            # Send webhook notification on successful completion
            await webhook_service.notify_job_completion(
                task_id=task_id,
                task_type="crawl",
                status="completed",
                urls=urls,
                webhook_config=webhook_config,
                result=result
            )

            await asyncio.sleep(5)  # Give Redis time to process the update
        except Exception as exc:
            await redis.hset(f"task:{task_id}", mapping={
                "status": TaskStatus.FAILED,
                "error": str(exc),
            })

            # Send webhook notification on failure
            await webhook_service.notify_job_completion(
                task_id=task_id,
                task_type="crawl",
                status="failed",
                urls=urls,
                webhook_config=webhook_config,
                error=str(exc)
            )

    background_tasks.add_task(_runner)
    return {"task_id": task_id}

async def clean_recommendations_with_llm(markdown_content: str, config: dict) -> str:
    """
    使用 LLM 清理推荐内容和外部链接

    Args:
        markdown_content: Pruning 过滤后的 markdown 内容
        config: 配置对象

    Returns:
        清理后的 markdown 内容
    """
    try:
        # 创建 LLM Content Filter
        llm_filter = LLMContentFilter(
            llm_config=LLMConfig(
                provider=config["llm"]["provider"],
                api_token=get_llm_api_key(config),
                base_url=get_llm_base_url(config),
                temperature=0.0,
            ),
            instruction="""
            你是一个推荐内容清理专家。你的任务是从已经清洗过的文章中删除推荐类内容和外部链接。

            ## 你的任务：

            在已经去除了导航、页脚、侧边栏等结构性内容的基础上，进一步清理：

            ### 必须删除的内容：

            1. **推荐类内容**（包含以下关键词的区块）：
                - 为你推荐、推荐阅读、推荐文章
                - 相关文章、相关阅读、相关推荐
                - 猜你喜欢、你可能喜欢、你还想看
                - 热门推荐、热门文章、热门话题
                - 最新文章、最新动态
                - 更多精彩、更多内容、阅读更多
                - 延伸阅读、扩展阅读、往期精彩

            2. **指向外部的内容**：
                - 包含链接列表的区块（多个外部链接标题）
                - "继续浏览"、"查看更多"、"点击查看"
                - "参考阅读"、"参考资料"（如果是外部链接）

            3. **引导性内容**：
                - "关注我们"、"订阅"、"扫码关注"
                - "分享给朋友"、"转发到朋友圈"
                - "点赞支持"、"收藏本页"

            ### 必须保留的内容：

            1. **当前页面的核心正文**：
                - 文章标题和副标题
                - 所有正文段落
                - 列表、表格、代码块

            2. **内部引用**：
                - 文章内部的章节引用
                - 文章内部的数据、图表说明

            3. **补充说明**：
                - 图片说明
                - 重要提示、警告
                - 作者信息、发布时间

            ### 处理原则：

            1. **遇到推荐内容立即停止**：
                - 一旦识别出推荐区块，删除该区块及其后的所有内容
                - 推荐内容通常出现在正文结束后

            2. **链接判断标准**：
                - 如果是当前文章的内部引用（如"见上文"、"如下图"）→ 保留
                - 如果是指向其他文章/网站的链接 → 删除整个区块

            3. **保留完整性**：
                - 不要截断正在阅读的正文段落
                - 确保删除后的内容仍然连贯

            4. **保守策略**：
                - 如果不确定是否是推荐内容，检查上下文
                - 如果看起来像正文的一部分，保留

            ### 输出要求：

            - 直接输出清理后的 Markdown
            - 不需要解释你删除了什么
            - 保持原有格式（标题、列表、代码块）
            - 如果整篇文章都是推荐内容，输出原文（不要全删）

            ## 特殊情况：

            **如果文章很短**：可能是单页应用，尽可能保留所有内容

            **如果内容很乱**：优先保留看起来像正文的部分，删除明显的链接列表

            **如果不确定**：保留！宁可多保留一些，也不要误删正文
            """,
            # chunk_token_threshold=16384,
            verbose=False,
        )

        # 使用 LLM 过滤
        cleaned_content = llm_filter.filter_content(markdown_content)

        # 如果 LLM 返回的是列表，合并成字符串
        if isinstance(cleaned_content, list):
            cleaned_content = "\n\n".join(cleaned_content)

        return cleaned_content if cleaned_content else markdown_content

    except Exception as e:
        logger.error(f"LLM cleaning error: {str(e)}")
        # 如果 LLM 失败，返回原内容
        return markdown_content

async def handle_subweb_crawl_request(urls: List[str]) -> dict:
    """Handle sub web crawl requests ."""
    try:
        # 检查输入格式
        has_titles = any("title:" in url and "url:" in url for url in urls)

        # 加载配置（两种格式都需要的配置）
        config = load_config()
        browser_config = BrowserConfig(
            extra_args=config["crawler"]["browser"].get("extra_args", []),
            **config["crawler"]["browser"].get("kwargs", {}),
            verbose=True,
        )

        prune_filter = PruningContentFilter(
            threshold=0.5,
            threshold_type="fixed",
        )

        crawlconfig = CrawlerRunConfig(
            word_count_threshold=10,
            excluded_tags=["nav", "footer", "header", "aside", "sidebar"],
            exclude_external_links=False,
            markdown_generator=DefaultMarkdownGenerator(
                # content_filter=llm_filter,
                content_filter=prune_filter,
                options={"ignore_links": False, "escape_html": False, "body_width": 80},
            ),
            scraping_strategy=LXMLWebScrapingStrategy(),
            cache_mode=CacheMode.WRITE_ONLY,
            stream=False,
            page_timeout=30000,
            delay_before_return_html=3.0,
        )

        dispatcher = MemoryAdaptiveDispatcher(
            memory_threshold_percent=config["crawler"]["memory_threshold_percent"],
            rate_limiter=RateLimiter(
                base_delay=tuple(config["crawler"]["rate_limiter"]["base_delay"])
            ),
        )

        from crawler_pool import get_crawler

        crawler = await get_crawler(browser_config)

        # 新增：处理单个 URL 的函数
        async def crawl_single_url(url: str):
            # 检测域名
            domain = extract_domain_from_url(url)
            # 检查是否有 Token 配置
            token_config = load_token_for_domain(domain) if domain else None
            if token_config:
                logger.info(f"Using token auth for {domain}")
                # 生成 Hooks
                hooks_code = generate_hooks_code(token_config)
                # 附加 Hooks
                from hook_manager import attach_user_hooks_to_crawler, UserHookManager

                hook_manager = UserHookManager(timeout=30)
                hooks_status, hook_manager = await attach_user_hooks_to_crawler(
                    crawler, hooks_code, timeout=30, hook_manager=hook_manager
                )
                # 获取配置文件中的额外参数
                crawler_config_overrides = get_crawler_config_overrides(token_config)
                # 基于默认配置，只覆盖配置文件中的参数
                if crawler_config_overrides:
                    for key, value in crawler_config_overrides.items():
                        if hasattr(crawlconfig, key):
                            setattr(crawlconfig, key, value)
            else:
                logger.info(
                    f"Using profile/default auth for {domain if domain else url}"
                )

            return await crawler.arun(url=url, config=crawlconfig)

        if has_titles:
            # 带标题的格式处理
            formatted_urls = []
            title_url_pairs = []

            for item in urls:
                # 解析带标题的格式
                parts = item.split("url:", 1)
                title_part = parts[0]
                url_part = parts[1] if len(parts) > 1 else ""

                # 提取标题
                original_title = title_part.split("title:", 1)[1].strip()

                # 添加唯一标识符（使用UUID的前8位）确保标题唯一
                unique_id = uuid4().hex[:8]
                title = f"{original_title} [{unique_id}]"

                # 提取URL
                url = url_part.strip()
                formatted_urls.append(url)
                title_url_pairs.append((title, url))

            # 确保所有URL都有协议前缀
            formatted_urls = [
                (
                    ("https://" + url)
                    if not url.startswith(("http://", "https://"))
                    and not url.startswith(("raw:", "raw://"))
                    else url
                )
                for url in formatted_urls
            ]

            results = []
            for url in formatted_urls:
                result = await crawl_single_url(url)
                results.append(result)

            # results = await crawler.arun_many(
            #     urls=formatted_urls, config=crawlconfig, dispatcher=dispatcher
            # )

            # 构建带标题的结果字典 - 并行 LLM 处理
            # 第一步：收集所有需要清理的内容
            tasks_data = []
            for i, (title, original_url) in enumerate(title_url_pairs):
                if i < len(results) and results[i].markdown:
                    content = results[i].markdown.fit_markdown
                    tasks_data.append((i, title, original_url, content))

            # 第二步：并行执行所有 LLM 清理任务
            logger.info(f"[LLM Parallel] 开始并行清理 {len(tasks_data)} 个网页内容...")
            llm_tasks = [
                clean_recommendations_with_llm(content, config)
                for _, _, _, content in tasks_data
            ]
            cleaned_contents = await asyncio.gather(*llm_tasks)
            logger.info(
                f"[LLM Parallel] 并行清理完成！共处理 {len(cleaned_contents)} 个网页"
            )

            # 第三步：组装结果
            web_pages = {}
            for (i, title, original_url, _), cleaned_content in zip(
                tasks_data, cleaned_contents
            ):
                web_pages[f"web_page{i+1}"] = {
                    "title": title,
                    "url": original_url,
                    "page_content": cleaned_content,
                }

            response = {"success": True, "results": web_pages, "format": "titled"}
            return response

        else:
            # 原始简单URL格式处理
            processed_urls = [
                (
                    ("https://" + url)
                    if not url.startswith(("http://", "https://"))
                    and not url.startswith(("raw:", "raw://"))
                    else url
                )
                for url in urls
            ]

            results = await crawler.arun_many(
                urls=processed_urls, config=crawlconfig, dispatcher=dispatcher
            )

            # 处理所有结果
            markdown_results = []
            for result in results:
                markdown = result.markdown
                if markdown:
                    markdown_results.append(markdown.fit_markdown)

            response = {
                "success": True,
                "results": markdown_results,
                "format": "simple",
            }
            return response

    except Exception as e:
        return {"success": False, "error": str(e)}