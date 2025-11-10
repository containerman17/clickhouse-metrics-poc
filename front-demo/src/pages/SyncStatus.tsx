import PageTransition from '../components/PageTransition';

function SyncStatus() {
  return (
    <PageTransition>
      <div className="p-8">
        <h1 className="text-3xl font-bold text-gray-900 mb-4">Sync Status</h1>
        <div className="bg-white rounded-lg shadow p-6">
          <p className="text-gray-600">Sync status page content will go here.</p>
        </div>
      </div>
    </PageTransition>
  );
}

export default SyncStatus;

