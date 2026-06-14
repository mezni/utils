// @ts-check
// Cross-platform compilation verification script.
// Run with: npx tsx scripts/verify-platforms.ts
// Confirms @bm/api-client compiles under both DOM and React Native lib targets.

import { execSync } from 'child_process'
import { writeFileSync, unlinkSync } from 'fs'
import { join } from 'path'

const root = join(import.meta.dirname, '..')
const tsconfigPath = join(root, 'tsconfig.json')

const platforms = [
  { name: 'DOM (web)', lib: '["ES2022","DOM","DOM.Iterable"]' },
  { name: 'React Native', lib: '["ES2022"]', types: '["react-native"]' },
]

let allPassed = true

for (const platform of platforms) {
  const config = {
    extends: './tsconfig.json',
    compilerOptions: {
      lib: JSON.parse(platform.lib),
      ...(platform.types ? { types: JSON.parse(platform.types) } : {}),
      noEmit: true,
    },
    include: ['src'],
  }

  const tempConfig = join(root, `tsconfig.${platform.name.toLowerCase().replace(/\s+/g, '-')}.json`)
  writeFileSync(tempConfig, JSON.stringify(config, null, 2))

  try {
    execSync(`npx tsc -p "${tempConfig}" --noEmit`, { cwd: root, stdio: 'pipe' })
    console.log(`✅ ${platform.name}: compiles successfully`)
  } catch (e) {
    console.error(`❌ ${platform.name}: compilation failed`)
    console.error(e.stderr?.toString() || e.message)
    allPassed = false
  } finally {
    unlinkSync(tempConfig)
  }
}

if (allPassed) {
  console.log('\n✅ All platforms pass')
  process.exit(0)
} else {
  console.error('\n❌ Some platforms failed')
  process.exit(1)
}
