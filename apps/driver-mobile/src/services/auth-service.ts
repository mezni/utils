import AsyncStorage from '@react-native-async-storage/async-storage';

const AUTH_TOKEN_KEY = 'auth_token';
const USER_DATA_KEY = 'user_data';

export class AuthService {
  /**
   * Login with credentials
   */
  static async login(credentials: any): Promise<any> {
    try {
      // TODO: Implement actual login via auth client
      // const response = await authClient.login(credentials);
      
      // Mock login for development
      const token = 'mock_token';
      const user = { 
        id: 'user-123',
        name: 'Driver',
        email: credentials.email || 'driver@example.com',
      };

      await AsyncStorage.setItem(AUTH_TOKEN_KEY, token);
      await AsyncStorage.setItem(USER_DATA_KEY, JSON.stringify(user));

      return { token, user };
    } catch (error) {
      console.error('Login failed:', error);
      throw error;
    }
  }

  /**
   * Logout
   */
  static async logout(): Promise<void> {
    try {
      // TODO: Call logout endpoint
      await AsyncStorage.removeItem(AUTH_TOKEN_KEY);
      await AsyncStorage.removeItem(USER_DATA_KEY);
    } catch (error) {
      console.error('Logout failed:', error);
      throw error;
    }
  }

  /**
   * Get auth token
   */
  static async getToken(): Promise<string | null> {
    try {
      return await AsyncStorage.getItem(AUTH_TOKEN_KEY);
    } catch (error) {
      console.error('Failed to get auth token:', error);
      return null;
    }
  }

  /**
   * Get user data
   */
  static async getUser(): Promise<any | null> {
    try {
      const userData = await AsyncStorage.getItem(USER_DATA_KEY);
      return userData ? JSON.parse(userData) : null;
    } catch (error) {
      console.error('Failed to get user:', error);
      return null;
    }
  }

  /**
   * Clear auth data
   */
  static async clearAuth(): Promise<void> {
    try {
      await AsyncStorage.removeItem(AUTH_TOKEN_KEY);
      await AsyncStorage.removeItem(USER_DATA_KEY);
    } catch (error) {
      console.error('Failed to clear auth:', error);
    }
  }
}

export default AuthService;
