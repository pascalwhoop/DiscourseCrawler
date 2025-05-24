import axios from 'axios'
import { RateLimiterMemory } from 'rate-limiter-flexible'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

import { DiscourseCrawler } from '../crawler.js'
import { Database } from '../database.js'

vi.mock('axios')
vi.mock('../database')
vi.mock('rate-limiter-flexible')

describe('DiscourseCrawler', () => {
  beforeEach(() => {
    vi.clearAllMocks
  })

  afterEach(() => {
    vi.resetAllMocks
  })

  describe('create', () => {
    it('should create a new DiscourseCrawler instance', async () => {
      const mockDb = { close: vi.fn() }

      vi.mocked(Database.create).mockResolvedValue(mockDb as any)

      const crawler = await DiscourseCrawler.create('https://test.com', 'test.db')

      expect(Database.create).toHaveBeenCalledWith('test.db')
      expect(crawler).toBeInstanceOf(DiscourseCrawler)
    })
  })

  describe('limitedFetch', () => {
    it('should fetch data with rate limiting', async () => {
      const mockDb = { close: vi.fn() }
      vi.mocked(Database.create).mockResolvedValue(mockDb as any)
      vi.mocked(RateLimiterMemory.prototype.consume).mockResolvedValue({} as any)
      vi.mocked(axios.get).mockResolvedValue({ data: { test: 'data' } })

      const crawler = await DiscourseCrawler.create('https://test.com')
      const result = await (crawler as any).limitedFetch('https://test.com')

      expect(RateLimiterMemory.prototype.consume).toHaveBeenCalledWith('default')
      expect(axios.get).toHaveBeenCalledWith('https://test.com')
      expect(result).toEqual({ test: 'data' })
    })

    it('should handle fetch errors', async () => {
      const mockDb = { close: vi.fn() }

      vi.mocked(Database.create).mockResolvedValue(mockDb as any)
      vi.mocked(RateLimiterMemory.prototype.consume).mockResolvedValue({} as any)
      vi.mocked(axios.get).mockRejectedValue(new Error('Network error'))

      const crawler = await DiscourseCrawler.create('https://test.com')
      await expect((crawler as any).limitedFetch('https://test.com')).rejects.toThrow()
    })
  })

  describe('getForum', () => {
    it('should return existing forum if found', async () => {
      const mockForum = {
        id: 1,
        url: 'https://test.com',
        categories_crawled: true,
      }

      const mockDb = {
        findForum: vi.fn().mockResolvedValue(mockForum),
        close: vi.fn(),
      }

      vi.mocked(Database.create).mockResolvedValue(mockDb as any)

      const crawler = await DiscourseCrawler.create('https://test.com')
      const result = await (crawler as any).getForum()

      expect(mockDb.findForum).toHaveBeenCalledWith('https://test.com')
      expect(result).toEqual(mockForum)
    })

    it('should create a new forum if not found', async () => {
      const mockForum = {
        id: 1,
        url: 'https://test.com',
        categories_crawled: false,
      }

      const mockDb = {
        findForum: vi.fn().mockResolvedValue(null),
        createForum: vi.fn().mockResolvedValue(mockForum),
        close: vi.fn(),
      }

      vi.mocked(Database.create).mockResolvedValue(mockDb as any)

      const crawler = await DiscourseCrawler.create('https://test.com')
      const result = await (crawler as any).getForum()

      expect(mockDb.findForum).toHaveBeenCalledWith('https://test.com')
      expect(mockDb.createForum).toHaveBeenCalledWith({
        url: 'https://test.com',
        categories_crawled: false,
      })
      expect(result).toEqual(mockForum)
    })
  })

  describe('crawl', () => {
    it('should crawl the forum', async () => {
      const mockForum = {
        id: 1,
        url: 'https://test.com',
        categories_crawled: true,
      }
      const mockDb = {
        close: vi.fn(),
      }
      vi.mocked(Database.create).mockResolvedValue(mockDb as any)

      const crawler = await DiscourseCrawler.create('https://test.com')
      const getForum = vi.spyOn(crawler as any, 'getForum').mockResolvedValue(mockForum)
      const crawlForum = vi.spyOn(crawler as any, 'crawlForum').mockResolvedValue(undefined)

      await crawler.crawl()

      expect(getForum).toHaveBeenCalled()
      expect(crawlForum).toHaveBeenCalledWith(mockForum)
    })
  })

  describe('close', () => {
    it('should close the database connection', async () => {
      const mockDb = { close: vi.fn() }
      vi.mocked(Database.create).mockResolvedValue(mockDb as any)

      const crawler = await DiscourseCrawler.create('https://test.com')
      await crawler.close()

      expect(mockDb.close).toHaveBeenCalled()
    })
  })

  describe('crawl with subpath', () => {
    it('should construct URLs correctly when base URL has a subpath', async () => {
      const baseUrlWithSubpath = 'http://localhost:4200/testforum'
      // The constructor normalizes by adding a trailing slash if there's a pathname
      const normalizedBaseUrl = 'http://localhost:4200/testforum/'
      const expectedCategoriesUrl = 'http://localhost:4200/testforum/categories.json'
      // Assuming category ID 1, page 0 for the first category page fetch
      const expectedCategory1Url = 'http://localhost:4200/testforum/c/1.json?page=0'

      const mockedAxiosGet = vi.mocked(axios.get)

      mockedAxiosGet.mockImplementation(async (url: string) => {
        // console.log(`axios.get mock called with: ${url}`) // For debugging
        if (url === expectedCategoriesUrl) {
          return Promise.resolve({
            data: {
              category_list: {
                categories: [
                  { id: 1, topic_url: 't/category-1/1', slug: 'category-1', name: 'Category 1' },
                ],
              },
            },
          })
        }
        if (url === expectedCategory1Url) {
          return Promise.resolve({
            data: {
              topic_list: { topics: [], more_topics_url: null },
            },
          })
        }
        // Fallback for any other URLs, like topic.json or posts.json, which we are not specifically testing here
        return Promise.resolve({ data: { post_stream: { posts: [], stream: [] } } })
      })

      const mockDbInstance = {
        findForum: vi.fn().mockResolvedValue(null),
        createForum: vi.fn().mockResolvedValue({ id: 1, url: normalizedBaseUrl, categories_crawled: false }),
        updateForum: vi.fn().mockResolvedValue(undefined),
        createCategory: vi.fn().mockResolvedValue({ id: 1, category_id: 1, forum_id: 1, pages_crawled: false, topic_url: 't/category-1/1' }),
        // This will be called twice: once before categories created (empty), once after for crawlCategory loop
        findCategoriesByForumId: vi.fn()
          .mockResolvedValueOnce([]) // Initial call in crawlForum before categories are processed
          .mockResolvedValueOnce([{ id: 1, category_id: 1, forum_id: 1, pages_crawled: false, topic_url: 't/category-1/1', json: '{}' }]), // For the crawlCategory loop
        getLastPageByCategory: vi.fn().mockResolvedValue(null),
        createPage: vi.fn().mockResolvedValue({ id: 1, page_id: 0, more_topics_url: null }),
        updateCategory: vi.fn().mockResolvedValue(undefined),
        getTopicsByCategoryId: vi.fn().mockResolvedValue([]), // No topics to crawl for simplicity
        getLatestTopicTimestamp: vi.fn().mockResolvedValue(null), // No existing data
        findTopicByCategoryIdAndTopicId: vi.fn().mockResolvedValue(null),
        close: vi.fn(),
        query: vi.fn().mockResolvedValue(undefined) // For resetCrawledState if fullCrawl is used
      }
      vi.mocked(Database.create).mockResolvedValue(mockDbInstance as any)

      const crawler = await DiscourseCrawler.create(baseUrlWithSubpath, 'test-subpath.db', { rateLimit: 0 }) // rateLimit 0 to speed up test
      await crawler.crawl()

      // Verify createForum was called with the normalized URL
      expect(mockDbInstance.createForum).toHaveBeenCalledWith({
        url: normalizedBaseUrl,
        categories_crawled: false,
      })

      // Verify axios.get was called for categories.json
      expect(mockedAxiosGet).toHaveBeenCalledWith(expectedCategoriesUrl)

      // Verify axios.get was called for the first category's page
      expect(mockedAxiosGet).toHaveBeenCalledWith(expectedCategory1Url)
      
      // Check that createCategory was called for category 1
      expect(mockDbInstance.createCategory).toHaveBeenCalledWith(
        expect.objectContaining({ category_id: 1, forum_id: 1 })
      );

      // Check that createPage was called for category 1, page 0
      expect(mockDbInstance.createPage).toHaveBeenCalledWith(
        expect.objectContaining({ category_id: 1, page_id: 0 })
      );
    })
  })
})
