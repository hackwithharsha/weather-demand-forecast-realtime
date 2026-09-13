import { Routes, Route, Navigate } from 'react-router-dom';
import NavBar from './components/NavBar';
import LiveView from './views/LiveView';
import PipelineView from './views/PipelineView';
import ModelView from './views/ModelView';
import DriftView from './views/DriftView';

export default function App() {
  return (
    <div className="min-h-screen bg-gray-950">
      <NavBar />
      <main className="max-w-7xl mx-auto px-6 py-6">
        <Routes>
          <Route path="/"          element={<LiveView />}     />
          <Route path="/pipeline"  element={<PipelineView />} />
          <Route path="/model"     element={<ModelView />}    />
          <Route path="/drift"     element={<DriftView />}    />
          <Route path="*"          element={<Navigate to="/" replace />} />
        </Routes>
      </main>
    </div>
  );
}
