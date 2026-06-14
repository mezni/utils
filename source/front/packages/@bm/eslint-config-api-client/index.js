module.exports = {
  rules: {
    // Prevent direct fetch/axios usage in app code — all HTTP must go through @bm/api-client
    'no-restricted-globals': [
      'error',
      {
        name: 'fetch',
        message: 'Use @bm/api-client instead of fetch directly. All HTTP traffic must go through the shared API client.',
      },
    ],
    'no-restricted-imports': [
      'error',
      {
        paths: [
          {
            name: 'axios',
            message: 'Use @bm/api-client instead of axios. All HTTP traffic must go through the shared API client.',
          },
        ],
        patterns: [
          {
            group: ['axios*'],
            message: 'Use @bm/api-client instead of axios. All HTTP traffic must go through the shared API client.',
          },
        ],
      },
    ],
  },
}
