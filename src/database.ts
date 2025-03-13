import { DuckDBConnection, DuckDBInstance } from '@duckdb/node-api'

import { Category, Forum, Page, Post, Topic } from './types/types.js'

/**
 * Database class for managing Discourse forum data in DuckDB
 * Handles database connections, schema creation, and CRUD for forum entities
 */
export class Database {
  private db: DuckDBInstance
  private connection: DuckDBConnection
  private initialized: boolean = false

  private constructor() {}

  /**
   * Creates a new Database instance and initializes the connection
   * @param {string} dbPath - Path to the DuckDB database file (defaults to 'discourse.db')
   * @returns {Promise<Database>} A configured Database instance
   */
  public static async create(dbPath: string = './discourse.db'): Promise<Database> {
    const instance = new Database()
    await instance.init(dbPath)
    return instance
  }

  /**
   * Initializes the database connection and creates the schema if missing
   * @param dbPath - Path to the DuckDB database file
   */
  async init(dbPath: string = './discourse.db') {
    const config = {
      path: dbPath,
    }
    this.db = await DuckDBInstance.create(config.path)
    this.connection = await this.db.connect()

    if (!this.initialized) {
      // Create tables if they don't exist
      await this.connection.run(`

        CREATE SEQUENCE IF NOT EXISTS forum_seq;

        CREATE TABLE IF NOT EXISTS forum (
          id INTEGER PRIMARY KEY DEFAULT nextval('forum_seq'),
          url VARCHAR,
          categories_crawled BOOLEAN DEFAULT FALSE
        );
        
        CREATE SEQUENCE IF NOT EXISTS user_seq;
        
        CREATE TABLE IF NOT EXISTS user (
          id INTEGER PRIMARY KEY DEFAULT nextval('user_seq'),
          forum_id INTEGER,
          json TEXT,
          FOREIGN KEY (forum_id) REFERENCES forum (id)
        );
        
        CREATE SEQUENCE IF NOT EXISTS category_seq;
        
        CREATE TABLE IF NOT EXISTS category (
          id INTEGER PRIMARY KEY DEFAULT nextval('category_seq'),
          category_id INTEGER,
          forum_id INTEGER,
          topic_url VARCHAR,
          json TEXT,
          pages_crawled BOOLEAN DEFAULT FALSE,
          UNIQUE (category_id, forum_id),
          FOREIGN KEY (forum_id) REFERENCES forum (id)
        );
        
        CREATE SEQUENCE IF NOT EXISTS page_seq;
        
        CREATE TABLE IF NOT EXISTS page (
          id INTEGER PRIMARY KEY DEFAULT nextval('page_seq'),
          page_id INTEGER,
          category_id INTEGER,
          more_topics_url VARCHAR,
          json TEXT,
          UNIQUE (category_id, page_id),
          FOREIGN KEY (category_id) REFERENCES category (id)
        );
        
        CREATE SEQUENCE IF NOT EXISTS topic_seq;
        
        CREATE TABLE IF NOT EXISTS topic (
          id INTEGER PRIMARY KEY DEFAULT nextval('topic_seq'),
          topic_id INTEGER,
          category_id INTEGER,
          page_excerpt_json TEXT,
          topic_json TEXT,
          posts_crawled BOOLEAN DEFAULT FALSE,
          last_crawled_at TIMESTAMP,
          UNIQUE (category_id, topic_id),
          FOREIGN KEY (category_id) REFERENCES category (id)
        );
        
        CREATE SEQUENCE IF NOT EXISTS post_seq;
        
        CREATE TABLE IF NOT EXISTS post (
          id INTEGER PRIMARY KEY DEFAULT nextval('post_seq'),
          post_id INTEGER,
          topic_id INTEGER,
          json TEXT,
          UNIQUE (topic_id, post_id),
          FOREIGN KEY (topic_id) REFERENCES topic (id)
        );
      `)

      this.initialized = true
    }
  }

  /**
   * Finds a forum by its URL
   * @param {string} url - The forum URL to search for
   * @returns {Promise<Forum | null>} The forum, if found, or null
   */
  async findForum(url: string): Promise<Forum | null> {
    const reader = await this.connection.runAndReadAll(
      `
          SELECT *
          FROM forum
          WHERE url = ?
          LIMIT 1
      `,
      [url],
    )

    // const rows = reader.getRows()
    const rows = reader.getRowObjects()

    return rows.length > 0 ? (rows[0] as unknown as Forum) : null
  }

  /**
   *
   * @param {Forum} forum - The Forum data to insert
   * @returns {Promise<Forum>} The created Forum with ID
   */
  async createForum(forum: Forum): Promise<Forum> {
    const reader = await this.connection.runAndReadAll(
      `
          INSERT INTO forum (url, categories_crawled)
          VALUES (?, ?)
          RETURNING *
      `,
      [forum.url, forum.categories_crawled],
    )

    const rows = reader.getRowObjects()
    return rows[0] as unknown as Forum
  }

  /**
   * Creates a new Forum record in the database
   * @param {number} id - The Forum ID to update
   * @param {Partial<Forum>} data - The Forum data to update
   */
  async updateForum(id: number, data: Partial<Forum>): Promise<void> {
    const setStatements: string[] = []
    const values: any[] = []

    if (data.url !== undefined) {
      setStatements.push('url = ?')
      values.push(data.url)
    }

    if (data.categories_crawled !== undefined) {
      setStatements.push(`categories_crawled = ?`)
      values.push(data.categories_crawled)
    }

    // If no data, return early
    if (setStatements.length === 0) return

    values.push(id)

    await this.connection.run(
      `
          UPDATE forum
          SET ${setStatements.join(', ')}
          WHERE id = ?
      `,
      [...values],
    )
  }

  /**
   * Finds all Categories belonging to a Forum
   * @param {number} forumId - The Forum ID to search
   * @return {Promise<Category[]>} - List of categories
   */
  async findCategoriesByForumId(forumId: number): Promise<Category[]> {
    const reader = await this.connection.runAndReadAll(
      `
          SELECT *
          FROM category
          WHERE forum_id = ?
      `,
      [forumId],
    )

    return reader.getRowObjects() as unknown as Category[]
  }

  /**
   * Finds a single Category by its Category ID and Forum ID
   * @param {number} categoryId - The Category ID to search
   * @param {number} forumId - The Forum ID to search
   * @returns {Promise<Category | null>} The Category, if found, or null
   */
  async findCategoryByCategoryIdAndForumId(
    categoryId: number,
    forumId: number,
  ): Promise<Category | null> {
    const reader = await this.connection.runAndReadAll(
      `
          SELECT *
          FROM category
          WHERE category_id = ?
            AND forum_id = ?
          LIMIT 1
      `,
      [categoryId, forumId],
    )

    const rows = reader.getRowObjects()
    return rows.length > 0 ? (rows[0] as unknown as Category) : null
  }

  /**
   * Creates a new Category record or returns existing record
   * @param {Category} category - The Category data to insert
   * @returns {Promise<Category>} The created or existing Category
   */
  async createCategory(category: Category): Promise<Category> {
    const existing = await this.findCategoryByCategoryIdAndForumId(
      category.category_id,
      category.forum_id,
    )

    if (existing) return existing

    const reader = await this.connection.runAndReadAll(
      `
          INSERT INTO category (category_id, forum_id, topic_url, json, pages_crawled)
          VALUES (?, ?, ?, ?, ?)
          RETURNING *
      `,
      [
        category.category_id,
        category.forum_id,
        category.topic_url,
        category.json,
        category.pages_crawled,
      ],
    )

    const rows = reader.getRowObjects()

    return rows[0] as unknown as Category
  }

  async updateCategory(id: number, data: Partial<Category>): Promise<void> {
    const setStatements: string[] = []
    const values: any[] = []

    if (data.topic_url !== undefined) {
      setStatements.push('topic_url = ?')
      values.push(data.topic_url)
    }

    if (data.json !== undefined) {
      setStatements.push('json = ?')
      values.push(data.json)
    }

    if (data.pages_crawled !== undefined) {
      setStatements.push(`pages_crawled = ?`)
      values.push(data.pages_crawled)
    }

    // If no data, return early
    if (setStatements.length === 0) return

    values.push(id)

    await this.connection.run(
      `
          UPDATE category
          SET ${setStatements.join(', ')}
          WHERE id = ?
      `,
      [...values],
    )
  }

  /**
   * Finds a topic by its Category ID and Topic ID
   * @param {number}  categoryId - The Category ID to search
   * @param {number} topicId - The Topic ID to search
   * @returns {Promise<Topic | null>} The topic, if found, or null
   */
  async findTopicByCategoryIdAndTopicId(
    categoryId: number,
    topicId: number,
  ): Promise<Topic | null> {
    const reader = await this.connection.runAndReadAll(
      `
          SELECT *
          FROM topic
          WHERE category_id = ?
            AND topic_id = ?
          LIMIT 1
      `,
      [categoryId, topicId],
    )

    const rows = reader.getRowObjects()
    return rows.length > 0 ? (rows[0] as unknown as Topic) : null
  }

  async getTopicsByCategoryId(categoryId: number): Promise<Topic[]> {
    const reader = await this.connection.runAndReadAll(
      `
          SELECT *
          FROM topic
          where category_id = ?
          ORDER BY topic_id desc
      `,
      [categoryId],
    )

    return reader.getRowObjects() as unknown as Topic[]
  }

  /**
   * Creates a new topic record or returns existing one if found.
   * @param {Topic} topic - The topic data to insert
   * @returns {Promise<Topic>} The created or existing topic
   */
  async createTopic(topic: Topic): Promise<Topic> {
    const existing = await this.findTopicByCategoryIdAndTopicId(
      topic.category_id,
      topic.category_id,
    )

    if (existing) return existing

    const reader = await this.connection.runAndReadAll(
      `
          INSERT INTO topic (topic_id, category_id, page_excerpt_json, topic_json, posts_crawled)
          VALUES (?, ?, ?, ?, ?)
          RETURNING *
      `,
      [
        topic.topic_id,
        topic.category_id,
        topic.page_excerpt_json,
        topic.topic_json,
        topic.posts_crawled,
      ],
    )

    const rows = reader.getRowObjects()

    return rows[0] as unknown as Topic
  }

  /**
   * Updates an existing topic record.
   * @param {number} id - The topic ID to update
   * @param {Partial<Topic>} data - The topic data to update
   */
  async updateTopic(id: number, data: Partial<Topic>): Promise<void> {
    const setStatements: string[] = []
    const values: any[] = []

    if (data.page_excerpt_json !== undefined) {
      setStatements.push('page_excerpt_json = ?')
      values.push(data.page_excerpt_json)
    }

    if (data.topic_json !== undefined) {
      setStatements.push('topic_json = ?')
      values.push(data.topic_json)
    }

    if (data.posts_crawled !== undefined) {
      setStatements.push(`posts_crawled = ?`)
      values.push(data.posts_crawled)
    }

    // If no data, return early
    if (setStatements.length === 0) return

    values.push(id)

    await this.connection.runAndReadAll(
      `
          UPDATE topic
          SET ${setStatements.join(', ')}
          WHERE id = ?
      `,
      [...values],
    )
  }

  /**
   * Updates last_crawled_at for a specific topic
   * @param {number} id
   */
  async updateTopicLastCrawled(id: number): Promise<void> {
    await this.connection.run(
      `
          UPDATE topic
          SET last_crawled_at = CURRENT_TIMESTAMP
          WHERE id = ?
      `,
      [id],
    )
  }

  async getTopicLastPostNumber(topicId: number): Promise<number | null> {
    const reader = await this.connection.runAndReadAll(
      `
          SELECT MAX(CAST(JSON_EXTRACT(json, '$.post_number') AS INTEGER)) as max_post_number
          FROM post
          WHERE topic_id = ?
      `,
      [topicId],
    )

    const rows = reader.getRowObjects()
    return (rows[0]?.max_post_number as number) || null
  }

  /**
   * Retrieves the most recent (latest) timestamp for crawled topics
   * @param {number} forumId
   * @returns {Promise<Date | null>}
   */
  async getLatestTopicTimestamp(forumId: number): Promise<Date | null> {
    const reader = await this.connection.runAndReadAll(
      `
          SELECT MAX(CAST(JSON_EXTRACT(topic_json, '$.created_at') AS TIMESTAMP)) as latest_timestamp
          FROM topic t
                   JOIN category c ON t.category_id = c.id
          WHERE c.forum_id = ?
      `,
      [forumId],
    )

    const rows = reader.getRowObjects()
    return rows[0]?.latest_timestamp ? new Date(rows[0].latest_timestamp as unknown as Date) : null
  }

  /**
   * Finds a post by its post ID and topic ID.
   * @param {number} postId - The post ID from Discourse
   * @param {number} topicId - The topic ID
   * @returns {Promise<Post | null>} The post if found, null otherwise
   */
  async findPostByPostIdAndTopicId(postId: number, topicId: number): Promise<Post | null> {
    const reader = await this.connection.runAndReadAll(
      `
          SELECT *
          FROM post
          WHERE post_id = ?
            AND topic_id = ?
          LIMIT 1
      `,
      [postId, topicId],
    )

    const rows = reader.getRowObjects()
    return rows.length > 0 ? (rows[0] as unknown as Post) : null
  }

  /**
   * Creates a new post record or returns existing one if found.
   * @param {Post} post - The post data to insert
   * @returns {Promise<Post>} The created or existing post
   */
  async createPost(post: Post): Promise<Post> {
    const existing = await this.findPostByPostIdAndTopicId(post.post_id, post.topic_id)

    if (existing) return existing

    const reader = await this.connection.runAndReadAll(
      `
          INSERT INTO post (post_id, topic_id, json)
          VALUES (?, ?, ?)
          RETURNING *
      `,
      [post.post_id, post.topic_id, post.json],
    )

    const rows = reader.getRowObjects()

    return rows[0] as unknown as Post
  }

  /**
   * Updates a Post if it has been edited
   * @param {number} id - The Post ID to update
   * @param {string} json - The new Post's JSON data
   * @returns {Promise<boolean>} True if updated, false if no change
   */
  async updatePostIfEdited(id: number, json: string): Promise<boolean> {
    const parsedJSON = JSON.parse(json)
    const versionNumber = parsedJSON.version || 1
    const editedAt = parsedJSON.updated_at || parsedJSON.created_at

    const reader = await this.connection.runAndReadAll(
      `
          SELECT id,
                 CAST(JSON_EXTRACT(json, '$.version') AS INTEGER) as version,
                 JSON_EXTRACT(json, '$.updated_at')               as updated_at
          FROM post
          WHERE id = ?
      `,
      [id],
    )

    const rows = reader.getRowObjects()

    if (rows.length === 0) return false

    const currentVersion = rows[0].version || 1
    const currentUpdatedAt = rows[0].updated_at as string

    if (
      versionNumber > currentVersion ||
      (editedAt && (currentUpdatedAt || new Date(editedAt) > new Date(currentUpdatedAt)))
    ) {
      await this.connection.run(
        `
            UPDATE post
            SET json = ?
            WHERE id = ?
        `,
        [json, id],
      )
      return true
    }

    return false
  }

  /**
   * Finds a page by its category ID and page ID.
   * @param {number} categoryId - The category ID
   * @param {number} pageId - The page ID
   * @returns {Promise<Page | null>} The page if found, null otherwise
   */
  async findPageByCategoryIdAndPageId(categoryId: number, pageId: number): Promise<Page | null> {
    const reader = await this.connection.runAndReadAll(
      `
          SELECT *
          FROM page
          WHERE category_id = ?
            AND page_id = ?
          LIMIT 1
      `,
      [categoryId, pageId],
    )

    const rows = reader.getRowObjects()

    return rows.length > 0 ? (rows[0] as unknown as Page) : null
  }

  /**
   * Creates a new page record or returns existing one if found.
   * @param {Page} page - The page data to insert
   * @returns {Promise<Page>} The created or existing page
   */
  async createPage(page: Page): Promise<Page> {
    const existing = await this.findPageByCategoryIdAndPageId(page.category_id, page.page_id)

    if (existing) return existing

    const reader = await this.connection.runAndReadAll(
      `
          INSERT INTO page (page_id, category_id, more_topics_url, json)
          VALUES (?, ?, ?, ?)
          RETURNING *
      `,
      [page.page_id, page.category_id, page.more_topics_url, page.json],
    )

    const rows = reader.getRowObjects()
    return rows[0] as unknown as Page
  }

  /**
   * Gets the last page (highest page_id) for a category.
   * @param {number} categoryId - The category ID
   * @returns {Promise<Page | null>} The last page if found, null otherwise
   */
  async getLastPageByCategory(categoryId: number): Promise<Page | null> {
    const reader = await this.connection.runAndReadAll(
      `
          SELECT *
          FROM page
          WHERE category_id = ?
          ORDER BY page_id DESC
          LIMIT 1
      `,
      [categoryId],
    )

    const rows = reader.getRowObjects()
    return rows.length > 0 ? (rows[0] as unknown as Page) : null
  }

  /**
   * Inserts multiple posts in a single transaction.
   * @param {Post[]} posts - Array of posts to insert
   */
  async bulkInsertPosts(posts: Post[]): Promise<void> {
    if (posts.length === 0) return

    const appender = await this.connection.createAppender('main', 'post')

    for (const post of posts) {
      appender.appendInteger(post.post_id)
      appender.appendInteger(post.topic_id)
      appender.appendVarchar(post.json)
      appender.endRow()
    }

    appender.close()
  }

  /**
   * Executes a custom SQL query with parameters.
   * @param {string} sql - SQL query to execute
   * @param {any[]} params - Query parameters
   * @returns {Promise<T[]>} Query results
   * @template T - Type of the returned data
   */
  async query<T>(sql: string, params: any[] = []): Promise<T[]> {
    const reader = await this.connection.runAndReadAll(sql, params)
    return reader.getRowObjects() as T[]
  }

  /**
   * Closes the database connection.
   */
  async close(): Promise<void> {
    if (this.connection) await this.connection.close()
  }
}
