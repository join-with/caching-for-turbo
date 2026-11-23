/** A provider that can only answer existence by downloading the artifact. */
export class ExistsNotSupportedError extends Error {
  constructor(provider: string) {
    super(`The ${provider} provider can't check existence without a download`)
    this.name = 'ExistsNotSupportedError'
  }
}
