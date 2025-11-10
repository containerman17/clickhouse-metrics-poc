import PageTransition from '../components/PageTransition';

function CustomSQL() {
  return (
    <PageTransition>
      <div className="p-8">
        <h1 className="text-3xl font-bold text-gray-900 mb-4">Custom SQL</h1>
        <div className="bg-white rounded-lg shadow p-6">
          <p className="text-gray-600">Custom SQL page content will go here.</p>
        </div>
      </div>
    </PageTransition>
  );
}

export default CustomSQL;

