type Variant = 'production' | 'staging' | 'archived' | 'none' | 'ok' | 'warn' | 'error';

const STYLES: Record<Variant, string> = {
  production: 'bg-emerald-900 text-emerald-300 border border-emerald-700',
  staging:    'bg-blue-900 text-blue-300 border border-blue-700',
  archived:   'bg-gray-800 text-gray-400 border border-gray-700',
  none:       'bg-gray-800 text-gray-400 border border-gray-700',
  ok:         'bg-emerald-900 text-emerald-300 border border-emerald-700',
  warn:       'bg-amber-900 text-amber-300 border border-amber-700',
  error:      'bg-red-900 text-red-300 border border-red-700',
};

function stageToVariant(stage: string): Variant {
  switch (stage.toLowerCase()) {
    case 'production': return 'production';
    case 'staging':    return 'staging';
    case 'archived':   return 'archived';
    default:           return 'none';
  }
}

interface BadgeProps {
  label: string;
  variant?: Variant;
  stage?: string;
}

export default function Badge({ label, variant, stage }: BadgeProps) {
  const v = variant ?? (stage ? stageToVariant(stage) : 'none');
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${STYLES[v]}`}>
      {label}
    </span>
  );
}
