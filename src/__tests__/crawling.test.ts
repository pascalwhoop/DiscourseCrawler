import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { DiscourseCrawler } from '../crawling'
import { Database } from '../database'
import axios from 'axios'
import { RateLimiterMemory } from 'rate-limiter-flexible'
import exp = require('node:constants')
import { Category, Forum, Topic } from '../types/types'

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
})
