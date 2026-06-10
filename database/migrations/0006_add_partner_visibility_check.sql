-- Enforce the partner visibility business rule at database level:
-- A partner cannot be marked as "live" unless they are also "verified".
-- This prevents the application from accidentally exposing unverified
-- stations to public drivers.
--
-- Constitution section 4: is_live = false OR is_verified = true
ALTER TABLE "ev-platform".partner
ADD CONSTRAINT ck_partner_live_requires_verified
CHECK (is_live = FALSE OR is_verified = TRUE);
