export enum LogLevel {
  DEBUG,
  INFO,
  WARN,
  ERROR,
  NONE,
}

export class Logger {
  private static instance: Logger;
  private currentLevel: LogLevel = LogLevel.NONE;
  private prefix: string = '[sms-classifier]';

  private constructor() {}

  static getInstance(): Logger {
    if (!Logger.instance) {
      Logger.instance = new Logger();
    }
    return Logger.instance;
  }

  setLevel(level: LogLevel): void {
    this.currentLevel = level;
  }

  setPrefix(prefix: string): void {
    this.prefix = prefix;
  }

  debug(...args: unknown[]): void {
    if (this.currentLevel <= LogLevel.DEBUG) {
      console.debug(this.prefix, ...args);
    }
  }

  info(...args: unknown[]): void {
    if (this.currentLevel <= LogLevel.INFO) {
      console.info(this.prefix, ...args);
    }
  }

  warn(...args: unknown[]): void {
    if (this.currentLevel <= LogLevel.WARN) {
      console.warn(this.prefix, ...args);
    }
  }

  error(...args: unknown[]): void {
    if (this.currentLevel <= LogLevel.ERROR) {
      console.error(this.prefix, ...args);
    }
  }
}

export const logger = Logger.getInstance();
