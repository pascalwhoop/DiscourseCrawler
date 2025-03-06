import { describe, it, expect, vi, beforeEach, afterEach} from 'vitest'
import {Database, Forum, Category, Page, Topic, Post} from '../database'
import {DuckDBInstance, DuckDBConnection} from '@duckdb/node-api'

// vi.mock('@duckdb/node-api', () => {
//   return {
//     DuckDBInstance: vi.fn().mockImplementation(() => {
//       connect: vi.fn().mockReturnValue({
//         runAndReadAll: vi.fn(),
//         run: vi.fn(),
//         close: vi.fn(),
//       })
//     })
//   }
// })

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
      const mockResult = {
        getRows: vi.fn().mockReturnValue([{
          id: 1,
          url: 'https://test.com',
          categories_crawled: true
        }])
      }
      (db as any).connection.runAndReadAll = vi.fn().mockResolvedValue(mockResult)

      const forum = await db.findForum("https://test.com")

      expect(forum).toEqual({
        id: 1,
        url: 'https://test.com',
        categories_crawled: true
      })

      expect((db as any).connection.runAndReadAll).toHaveBeenCalledWith(
        expect.stringContaining(
          "SELECT * FROM forum WHERE url = 'https://test.com'"
        )
      )

    })
  })
})
