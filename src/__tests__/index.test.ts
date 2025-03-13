import { describe, it, expect, vi } from 'vitest'
import { main } from '../index'

describe('main', () => {
  it('should log a greeting', () => {
    // Arrange
    const consoleSpy = vi.spyOn(console, 'log')

    // Act
    main()

    // Assert
    expect(consoleSpy).toHaveBeenCalledWith('Hello, TypeScript!')

    // Cleanup
    consoleSpy.mockRestore()
  })
})
