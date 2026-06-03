import Keycloak from 'keycloak-js'

let keycloak: Keycloak | null = null

function getKeycloak(): Keycloak {
  if (!keycloak) {
    keycloak = new Keycloak({
      url: 'http://localhost/auth',
      realm: 'bornemap',
      clientId: 'bornemap-api',
    })
  }
  return keycloak
}

let initPromise: Promise<boolean> | null = null

export async function initAuth(): Promise<boolean> {
  if (!initPromise) {
    initPromise = getKeycloak()
      .init({
        onLoad: 'check-sso',
        pkceMethod: 'S256',
        silentCheckSsoRedirectUri:
          window.location.origin + '/silent-check-sso.html',
        checkLoginIframe: true,
      })
      .then((authenticated) => authenticated)
      .catch(() => false)
  }
  return initPromise
}

export async function getToken(): Promise<string | null> {
  try {
    const kc = getKeycloak()
    if (!kc.authenticated) return null
    const refreshed = await kc.updateToken(5)
    if (refreshed) {
      console.log('Token refreshed')
    }
    return kc.token ?? null
  } catch {
    return null
  }
}

export async function login(provider?: string): Promise<void> {
  const kc = getKeycloak()
  if (provider) {
    await kc.login({ idpHint: provider })
  } else {
    await kc.login({ redirectUri: window.location.origin })
  }
}

export async function logout(): Promise<void> {
  const kc = getKeycloak()
  await kc.logout({ redirectUri: window.location.origin })
}

export function isAuthenticated(): boolean {
  return getKeycloak().authenticated ?? false
}

export function getUser(): { id?: string; email?: string; name?: string } | null {
  const kc = getKeycloak()
  if (!kc.authenticated || !kc.tokenParsed) return null
  return {
    id: kc.subject,
    email: kc.tokenParsed.email as string | undefined,
    name: kc.tokenParsed.name as string | undefined,
  }
}
