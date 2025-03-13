import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

import { Database } from '../database.js'
import { Forum } from '../types/types.js'

vi.mock('@duckdb/node-api', () => {
  const mockConnection = {
    runAndReadAll: vi.fn(),
    run: vi.fn(),
    close: vi.fn(),
  }

  const mockInstance = {
    connect: vi.fn().mockReturnValue(mockConnection),
  }

  return {
    DuckDBInstance: {
      create: vi.fn().mockReturnValue(mockInstance),
    },
  }
})

describe('Database', () => {
  let db: Database

  beforeEach(async () => {
    vi.clearAllMocks()
    db = await Database.create('test.db')
  })

  afterEach(async () => {
    await db.close()
  })

  describe('create', () => {
    it('should create a new Database instance', async () => {
      expect(db).toBeInstanceOf(Database)
    })
  })
  describe('Forum operations', () => {
    it('should find a forum by URL', async () => {
      // Arrange
      const mockForum = { id: 1, url: 'http://example.com', categories_crawled: true }
      const mockResult = {
        getRowObjects: vi.fn().mockReturnValue([mockForum]),
      }
      ;(db as any).connection.runAndReadAll = vi.fn().mockResolvedValue(mockResult)

      // Act
      const forum = await db.findForum('https://example.com')

      // Assert
      expect(forum).toEqual(mockForum)
      expect((db as any).connection.runAndReadAll).toHaveBeenCalledWith(
        expect.stringMatching(/SELECT.*FROM forum.*WHERE url = \?/s),
        ['https://example.com'],
      )
    })

    it('should return null if forum not found', async () => {
      const mockResult = {
        getRowObjects: vi.fn().mockReturnValue([]),
      }
      ;(db as any).connection.runAndReadAll = vi.fn().mockReturnValue(mockResult)

      const forum = await db.findForum('https://doesntexist.com')

      expect(forum).toBeNull()
    })

    it('should create a forum', async () => {
      const mockForum: Forum = { url: 'https://test.com', categories_crawled: false }
      const mockResult = {
        getRowObjects: vi.fn().mockReturnValue([{ id: 1, ...mockForum }]),
      }
      ;(db as any).connection.runAndReadAll = vi.fn().mockResolvedValue(mockResult)

      const result = await db.createForum(mockForum)

      expect(result).toEqual({ id: 1, ...mockForum })
      expect((db as any).connection.runAndReadAll).toHaveBeenCalledWith(
        expect.stringMatching(/INSERT INTO forum.*VALUES/s),
        ['https://test.com', false],
      )
    })
  })
  describe('Topics', () => {
    it('should find a topic by category ID and topic ID', async () => {
      const mockTopic = {
        id: 1,
        topic_id: 101,
        category_id: 1,
        page_excerpt_json: '{}',
        topic_json: null,
        posts_crawled: false,
      }
      const mockResult = {
        getRowObjects: vi.fn().mockReturnValue([mockTopic]),
      }
      ;(db as any).connection.runAndReadAll = vi.fn().mockResolvedValue(mockResult)

      const topic = await db.findTopicByCategoryIdAndTopicId(1, 101)

      expect(topic).toEqual(mockTopic)
      expect((db as any).connection.runAndReadAll).toHaveBeenCalledWith(
        expect.stringMatching(/SELECT.*FROM topic.*WHERE category_id = \?.*AND topic_id = \?/s),
        [1, 101],
      )
    })
  })
  describe('Post operations', () => {
    it('should find a post by post ID and topic ID', async () => {
      // Arrange
      const mockPost = { id: 1, post_id: 101, topic_id: 1, json: '{}' }
      const mockResult = {
        getRowObjects: vi.fn().mockReturnValue([mockPost]),
      }
      ;(db as any).connection.runAndReadAll = vi.fn().mockResolvedValue(mockResult)

      // Act
      const post = await db.findPostByPostIdAndTopicId(101, 1)

      // Assert
      expect(post).toEqual(mockPost)
      expect((db as any).connection.runAndReadAll).toHaveBeenCalledWith(
        expect.stringMatching(/SELECT.*FROM post.*WHERE post_id = \?.*AND topic_id = \?/s),
        [101, 1],
      )
    })
  })

  describe('Page operations', () => {
    it('should get the last page by category ID', async () => {
      // Arrange
      const mockPage = { id: 1, page_id: 2, category_id: 1, more_topics_url: null, json: '{}' }
      const mockResult = {
        getRowObjects: vi.fn().mockReturnValue([mockPage]),
      }
      ;(db as any).connection.runAndReadAll = vi.fn().mockResolvedValue(mockResult)

      // Act
      const page = await db.getLastPageByCategory(1)

      // Assert
      expect(page).toEqual(mockPage)
      expect((db as any).connection.runAndReadAll).toHaveBeenCalledWith(
        expect.stringMatching(/SELECT.*FROM page.*WHERE category_id = \?.*ORDER BY page_id DESC/s),
        [1],
      )
    })
  })
})
