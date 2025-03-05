import {DuckDBInstance, DuckDBConnection} from '@duckdb/node-api';

interface Forum {
  id?: number
  url: string
  categories_crawled: boolean
}

interface User {
  id?: number
  forum_id: number
  json: string
}

interface Category {
  id?: number
  category_id: number
  forum_id: number
  topic_url: string
  json: string
  pages_crawled: boolean
}

interface Page {
  id?: number
  page_id: number
  category_id: number
  more_topics_url: string | null
  json: string
}

interface Topic {
  id?: number
  topic_id: number
  category_id: number
  page_excerpt_json: string
  topic_json: string
  posts_crawled: boolean
}

interface Post {
  id?: number
  post_id: number
  topic_id: number
  json: string
}

export class Database {

  private db: DuckDBInstance
  private connection: DuckDBConnection
  private initialized: boolean = false

  constructor() {
  }

  async init(databasePath: string = 'discourse.db') {
    const config = {
      path: databasePath
    }
    //
    this.db = await DuckDBInstance.create(config.path)

    if (!this.initialized) {
      // Create tables if they don't exist
      await this.connection.run(`
          CREATE TABLE IF NOT EXISTS forum
          (
              id
              INTEGER
              PRIMARY
              KEY,
              url
              VARCHAR,
              categories_crawled
              BOOLEAN
              DEFAULT
              FALSE
          );

          CREATE TABLE IF NOT EXISTS user
          (
              id
              INTEGER
              PRIMARY
              KEY,
              forum_id
              INTEGER,
              json
              TEXT,
              FOREIGN
              KEY
          (
              forum_id
          ) REFERENCES forum
          (
              id
          )
              );

          CREATE TABLE IF NOT EXISTS category
          (
              id
              INTEGER
              PRIMARY
              KEY,
              category_id
              INTEGER,
              forum_id
              INTEGER,
              topic_url
              VARCHAR,
              json
              TEXT,
              pages_crawled
              BOOLEAN
              DEFAULT
              FALSE,
              UNIQUE
          (
              category_id,
              forum_id
          ),
              FOREIGN KEY
          (
              forum_id
          ) REFERENCES forum
          (
              id
          )
              );

          CREATE TABLE IF NOT EXISTS page
          (
              id
              INTEGER
              PRIMARY
              KEY,
              page_id
              INTEGER,
              category_id
              INTEGER,
              more_topics_url
              VARCHAR,
              json
              TEXT,
              UNIQUE
          (
              category_id,
              page_id
          ),
              FOREIGN KEY
          (
              category_id
          ) REFERENCES category
          (
              id
          )
              );

          CREATE TABLE IF NOT EXISTS topic
          (
              id
              INTEGER
              PRIMARY
              KEY,
              topic_id
              INTEGER,
              category_id
              INTEGER,
              page_excerpt_json
              TEXT,
              topic_json
              TEXT,
              posts_crawled
              BOOLEAN
              DEFAULT
              FALSE,
              UNIQUE
          (
              category_id,
              topic_id
          ),
              FOREIGN KEY
          (
              category_id
          ) REFERENCES category
          (
              id
          )
              );

          CREATE TABLE IF NOT EXISTS post
          (
              id
              INTEGER
              PRIMARY
              KEY,
              post_id
              INTEGER,
              topic_id
              INTEGER,
              json
              TEXT,
              UNIQUE
          (
              topic_id,
              post_id
          ),
              FOREIGN KEY
          (
              topic_id
          ) REFERENCES topic
          (
              id
          )
              );
      `);

      this.initialized = true

    }
  }

  async findForum(url: string): Promise<Forum | null> {
    const reader = await this.connection.runAndReadAll(`
    SELECT * FROM forum WHERE url = ? LIMIT 1
    `, [url])

    const rows = reader.getRows()
    return rows.length > 0 ? rows[0] as Forum : null
  }

  async createForum(forum: Forum): Promise<Forum> {
    const reader = await this.connection.runAndReadAll(`
    INSERT INTO forum (url, categories_crawled)
    VALUES (?, ?)
    RETURNING *
    `, [forum.url, forum.categories_crawled])

    const rows = reader.getRows()
    return rows[0] as Forum
  }

  async updateForum(id: number, data: Partial<Forum>): Promise<void> {
    const setStatements: string[] = []
    const values: any[] = []

    if (data.url !== undefined){
      setStatements.push('url = ?')
      values.push(data.url)
    }

    if(data.categories_crawled !== undefined) {
      setStatements.push(`categories_crawled = ?`)
      values.push(data.categories_crawled)
    }

    // If no data, return early
    if (setStatements.length === 0) return

    values.push(id)

    await this.connection.run(`
    UPDATE forum SET ${setStatements.join(', ')} WHERE id = ?
    `)
  }

}