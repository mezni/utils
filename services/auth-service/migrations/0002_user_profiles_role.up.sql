ALTER TABLE users.user_profiles
ADD COLUMN IF NOT EXISTS role VARCHAR(20) NOT NULL DEFAULT 'driver'
CHECK (role IN ('driver', 'partner', 'admin'));

CREATE INDEX IF NOT EXISTS idx_user_profiles_role
ON users.user_profiles (role);
