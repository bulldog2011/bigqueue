package com.leansoft.bigqueue.page;

import java.io.IOException;
import java.util.Set;

/**
 * Memory mapped page management ADT
 * 
 * @author bulldog
 *
 */
public interface IMappedPageFactory {
	
	/**
	 * Acquire a mapped page with specific index from the factory
	 * 
	 * @param index the index of the page
	 * @return a mapped page
	 * @throws IOException exception thrown if there was any IO error during the acquire operation
	 */
	IMappedPage acquirePage(long index) throws IOException;
	
	/**
	 * Return the mapped page to the factory,
	 * calling thread release the page to inform the factory that it has finished with the page,
	 * so the factory get a chance to recycle the page to save memory.
	 * 
	 * @param index the index of the page
	 */
	void releasePage(long index);
	
	/**
	 * Current set page size, when creating pages, the factory will
	 * only create pages with this size.
	 * 
	 * @return an integer number
	 */
	int getPageSize();
	
	/**
	 * Current set page directory.
	 * 
	 * @return
	 */
	String getPageDir();
	
	/**
	 * delete a mapped page with specific index in this factory,
	 * this call will remove the page from the cache if it is cached and
	 * delete back file.
	 * 
	 * @param index the index of the page
	 * @throws IOException exception thrown if there was any IO error during the delete operation.
	 */
	void deletePage(long index) throws IOException;
	
	/**
	 * delete mapped pages with a set of specific indexes in this factory,
	 * this call will remove the pages from the cache if they ware cached and
	 * delete back files.
	 * 
	 * @param indexes the indexes of the pages
	 * @throws IOException
	 */
	void deletePages(Set<Long> indexes) throws IOException;
	
	/**
	 * delete all mapped pages currently available in this factory,
	 * this call will remove all pages from the cache and delete all back files.
	 * 
	 * @throws IOException exception thrown if there was any IO error during the delete operation.
	 */
	void deleteAllPages() throws IOException;
	
	/**
	 * remove all cached pages from the cache and close resources associated with the cached pages.
	 * 
	 * @throws IOException exception thrown if there was any IO error during the release operation.
	 */
	void releaseCachedPages() throws IOException;
	
	/**
	 * Get all indexes of pages with last modified timestamp before the specific timestamp.
	 * 
	 * @param timestamp the timestamp to check
	 * @return a set of indexes
	 */
	Set<Long> getPageIndexSetBefore(long timestamp);

    Set<Long> getPageIndexSetAfter(long timestamp);

	/**
	 * Delete all pages with last modified timestamp before the specific timestamp.
	 * 
	 * @param timestamp the timestamp to check
	 * @throws IOException exception thrown if there was any IO error during the delete operation.
	 */
	void deletePagesBefore(long timestamp) throws IOException;
	
	/**
	 * Get last modified timestamp of page file index
	 * 
	 * @param index page index
	 */
	long getPageFileLastModifiedTime(long index);

	long getFirstPageIndexAfter(long timestamp);

	/**
	 * Get index of a page file with last modified timestamp closest to specific timestamp.
	 * 
	 * @param timestamp the timestamp to check
	 * @return a page index
	 */
	long getFirstPageIndexBefore(long timestamp);
	
	/**
	 * For test, get a list of indexes of current existing back files.
	 * 
	 * @return a set of indexes
	 */
	Set<Long> getExistingBackFileIndexSet();
	
	/**
	 * For test, get current cache size
	 * 
	 * @return an integer number
	 */
	int getCacheSize();
	
	/**
	 * Persist any changes in cached mapped pages
	 */
	void flush();
	
	/**
	 * 
	 * A set of back page file names
	 * 
	 * @return file name set
	 */
	Set<String> getBackPageFileSet();
	
	
	/**
	 * Total size of all page files
	 * 
	 * @return total size
	 */
	long getBackPageFileSize();
	
}
