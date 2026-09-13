import { NavLink } from 'react-router-dom';

const LINKS = [
  { to: '/',          label: 'Live'     },
  { to: '/pipeline',  label: 'Pipeline' },
  { to: '/model',     label: 'Model'    },
  { to: '/drift',     label: 'Drift'    },
] as const;

export default function NavBar() {
  return (
    <nav className="sticky top-0 z-40 h-12 bg-gray-900 border-b border-gray-800 flex items-center px-6 gap-1">
      <span className="mr-4 font-semibold text-gray-100 tracking-tight select-none">
        Demand Forecast
      </span>
      {LINKS.map(({ to, label }) => (
        <NavLink
          key={to}
          to={to}
          end={to === '/'}
          className={({ isActive }) =>
            [
              'px-3 py-1 rounded-md text-sm font-medium transition-colors',
              isActive
                ? 'bg-blue-600 text-white'
                : 'text-gray-400 hover:text-gray-100 hover:bg-gray-800',
            ].join(' ')
          }
        >
          {label}
        </NavLink>
      ))}
    </nav>
  );
}
