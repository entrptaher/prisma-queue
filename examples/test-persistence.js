const QueueFactory = require('../src/QueueFactory');

async function addJobs() {
  const queue = await QueueFactory.createQueue('testQueue');
  
  console.log('Adding jobs...');
  
  // Add several jobs
  await queue.add('test', { message: 'Job 1' });
  await queue.add('test', { message: 'Job 2' });
  await queue.add('test', { message: 'Job 3' });
  
  console.log('Jobs added. You can now start the worker in a separate process.');
  await queue.close();
}

addJobs().catch(console.error);