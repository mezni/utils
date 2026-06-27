import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { useNavigate } from 'react-router-dom'
import { loginSchema, type LoginFormData } from '@/lib/validation'
import { useLogin } from '@/hooks/use-auth'
import { motion } from 'framer-motion'

export function LoginPage() {
  const navigate = useNavigate()
  const login = useLogin()

  const {
    register,
    handleSubmit,
    formState: { errors },
  } = useForm<LoginFormData>({
    resolver: zodResolver(loginSchema),
  })

  const onSubmit = (data: LoginFormData) => {
    login.mutate(data, {
      onSuccess: () => navigate('/'),
    })
  }

  return (
    <div className="min-h-screen bg-background flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.2 }}
        className="w-full max-w-sm"
      >
        <div className="rounded-xl border border-border bg-muted/30 p-8">
          <div className="text-center mb-8">
            <h1 className="font-heading text-2xl font-bold text-foreground mb-2">
              BorneMap
            </h1>
            <p className="text-sm text-foreground/50">
              Sign in to the admin dashboard
            </p>
          </div>

          <form onSubmit={handleSubmit(onSubmit)} className="space-y-5" noValidate>
            <div>
              <label htmlFor="email" className="block text-sm font-medium text-foreground mb-1.5">
                Email
              </label>
              <input
                id="email"
                type="email"
                autoComplete="email"
                className={`w-full px-3 py-2.5 rounded-lg border bg-background text-foreground text-sm transition-colors duration-150 focus:outline-none focus:ring-2 focus:ring-ring ${
                  errors.email ? 'border-destructive' : 'border-border'
                }`}
                {...register('email')}
                aria-invalid={!!errors.email}
                aria-describedby={errors.email ? 'email-error' : undefined}
              />
              {errors.email && (
                <p id="email-error" className="mt-1 text-xs text-destructive" role="alert">
                  {errors.email.message}
                </p>
              )}
            </div>

            <div>
              <label htmlFor="password" className="block text-sm font-medium text-foreground mb-1.5">
                Password
              </label>
              <input
                id="password"
                type="password"
                autoComplete="current-password"
                className={`w-full px-3 py-2.5 rounded-lg border bg-background text-foreground text-sm transition-colors duration-150 focus:outline-none focus:ring-2 focus:ring-ring ${
                  errors.password ? 'border-destructive' : 'border-border'
                }`}
                {...register('password')}
                aria-invalid={!!errors.password}
                aria-describedby={errors.password ? 'password-error' : undefined}
              />
              {errors.password && (
                <p id="password-error" className="mt-1 text-xs text-destructive" role="alert">
                  {errors.password.message}
                </p>
              )}
            </div>

            {login.error && (
              <div className="p-3 rounded-lg bg-destructive/10 border border-destructive/30" role="alert">
                <p className="text-sm text-destructive">
                  {login.error.message || 'Invalid credentials'}
                </p>
              </div>
            )}

            <button
              type="submit"
              disabled={login.isPending}
              className="w-full py-2.5 px-4 rounded-lg bg-primary text-on-primary font-medium text-sm hover:brightness-110 transition-all duration-150 cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed focus:outline-none focus:ring-2 focus:ring-ring"
            >
              {login.isPending ? 'Signing in...' : 'Sign in'}
            </button>
          </form>
        </div>
      </motion.div>
    </div>
  )
}
