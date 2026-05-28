const { getDefaultConfig } = require('expo/metro-config');

const config = getDefaultConfig(__dirname);

const origResolveRequest = config.resolver.resolveRequest;
config.resolver.resolveRequest = (ctx, moduleName, platform) => {
  if (
    platform === 'web' &&
    moduleName === 'react-native/Libraries/Utilities/codegenNativeCommands'
  ) {
    return { type: 'empty' };
  }
  return origResolveRequest
    ? origResolveRequest(ctx, moduleName, platform)
    : ctx.resolveRequest(ctx, moduleName, platform);
};

module.exports = config;
