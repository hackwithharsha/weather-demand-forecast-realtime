import type { ReactNode } from 'react';

interface CardProps {
  title?: string;
  children: ReactNode;
  className?: string;
}

export default function Card({ title, children, className = '' }: CardProps) {
  return (
    <div className={`bg-gray-900 border border-gray-800 rounded-xl p-5 ${className}`}>
      {title && (
        <h2 className="text-xs font-semibold uppercase tracking-wider text-gray-400 mb-4">
          {title}
        </h2>
      )}
      {children}
    </div>
  );
}
