const { getDefaultConfig } = require('expo/metro-config')
const path = require('path')

const config = getDefaultConfig(__dirname)

config.watchFolders.push(
  path.resolve(__dirname, '../../packages')
)

config.resolver.nodeModulesPaths.unshift(
  path.resolve(__dirname, '../../node_modules')
)

module.exports = config
