import { useState, type ReactNode } from 'react';
import { Button } from '../ui/Button';

interface WizardStep {
  id: string;
  label: string;
  description: string;
  icon: ReactNode;
}

interface ProvisionWizardProps {
  steps: WizardStep[];
  currentStep: number;
  onStepChange: (step: number) => void;
  children: ReactNode;
  onComplete: () => void;
  onCancel: () => void;
  canProceed: boolean;
  loading?: boolean;
  completeLabel?: string;
}

const stepIcons = [
  <svg key="1" className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414m0 0a1 1 0 01.293.707V19a2 2 0 01-2 2z" /></svg>,
  <svg key="2" className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" /></svg>,
  <svg key="3" className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>,
  <svg key="4" className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" /></svg>,
];

export function ProvisionWizard({
  steps, currentStep, onStepChange, children, onComplete, onCancel,
  canProceed, loading, completeLabel = 'Provision Asset',
}: ProvisionWizardProps) {
  const isLast = currentStep === steps.length - 1;

  return (
    <div className="space-y-6">
      {/* Step indicators */}
      <div className="flex items-center gap-0">
        {steps.map((step, i) => (
          <div key={step.id} className="flex items-center flex-1">
            <button
              type="button"
              onClick={() => onStepChange(i)}
              className={`flex items-center gap-2 px-3 py-2 rounded-lg text-xs font-medium transition-all
                ${i === currentStep
                  ? 'text-orange-400 bg-orange-500/10'
                  : i < currentStep
                    ? 'text-green-400 hover:bg-gray-800/50'
                    : 'text-gray-600 hover:text-gray-400'}`}
            >
              <span className={`w-6 h-6 rounded-full flex items-center justify-center text-[10px] font-bold
                ${i === currentStep ? 'bg-orange-500 text-slate-900' : i < currentStep ? 'bg-green-500/20 text-green-400' : 'bg-gray-800 text-gray-600'}`}
              >
                {i < currentStep ? '✓' : i + 1}
              </span>
              <span className="hidden sm:inline">{step.label}</span>
            </button>
            {i < steps.length - 1 && <div className="flex-1 h-px mx-2 bg-gray-800" />}
          </div>
        ))}
      </div>

      {/* Step content */}
      <div className="bg-surface border border-gray-800 rounded-xl p-6 min-h-[300px]">
        {children}
      </div>

      {/* Navigation */}
      <div className="flex items-center justify-between">
        <Button variant="ghost" onClick={onCancel}>Cancel</Button>
        <div className="flex gap-3">
          {currentStep > 0 && (
            <Button variant="secondary" onClick={() => onStepChange(currentStep - 1)}>
              Back
            </Button>
          )}
          <Button
            onClick={isLast ? onComplete : () => onStepChange(currentStep + 1)}
            disabled={!canProceed}
            loading={loading}
            variant={isLast ? 'primary' : 'secondary'}
          >
            {isLast ? completeLabel : 'Continue'}
          </Button>
        </div>
      </div>
    </div>
  );
}
