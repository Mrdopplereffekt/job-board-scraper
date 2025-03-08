def get_url_chunks(careers_page_urls, chunk_size):
    """
    Split a list of URLs into chunks for parallel processing.
    Makes sure to return at least one chunk even if the input is empty.
    
    Args:
        careers_page_urls: List of tuples containing URLs
        chunk_size: Number of URLs per chunk
        
    Returns:
        List of lists, each containing URLs as strings
    """
    # Handle empty input
    if not careers_page_urls:
        return []
    
    # Ensure chunk_size is at least 1
    chunk_size = max(1, chunk_size)
    
    url_chunks = []
    single_chunk = []
    
    for i, url in enumerate(careers_page_urls):
        careers_page_url = url[0]  # UnTuple-ify
        single_chunk.append(careers_page_url)
        if i % chunk_size == chunk_size - 1:
            url_chunks.append(single_chunk)
            single_chunk = []
    
    # Add any remaining URLs as a final chunk
    if len(single_chunk) > 0:
        url_chunks.append(single_chunk)
        
    return url_chunks
