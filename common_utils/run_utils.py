import asyncio
import streamlit as st
import logging
import concurrent.futures

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def run_async(coro):
    """Run an async coroutine in a synchronous context using Streamlit's event loop."""
    try:
        loop = asyncio.get_running_loop()
        logger.debug(f"Scheduling task in running loop: {loop}")
    except RuntimeError:
        logger.warning("No running event loop; creating new loop")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # Configure thread pool if not already set
    if not hasattr(loop, '_default_executor'):
        loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(max_workers=4))
        logger.debug("Configured ThreadPoolExecutor with max_workers=4")

    task = loop.create_task(coro)
    if "async_tasks" not in st.session_state:
        st.session_state["async_tasks"] = []
    st.session_state["async_tasks"].append(task)
    task.add_done_callback(
        lambda t: st.session_state["async_tasks"].remove(t)
        if t in st.session_state["async_tasks"]
        else None
    )

    # If we created a new loop, run the coroutine synchronously
    if not loop.is_running():
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    return task