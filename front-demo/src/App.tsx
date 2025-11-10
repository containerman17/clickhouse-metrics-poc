import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Layout from './components/Layout';
import Metrics from './pages/Metrics';
import CustomSQL from './pages/CustomSQL';
import SyncStatus from './pages/SyncStatus';

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Navigate to="/metrics" replace />} />
          <Route path="metrics" element={<Metrics />} />
          <Route path="custom-sql" element={<CustomSQL />} />
          <Route path="sync-status" element={<SyncStatus />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}

export default App
