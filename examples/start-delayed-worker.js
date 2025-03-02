const QueueFactory = require('../src/QueueFactory');
const Worker = require('../src/Worker');

async function startWorker() {
  const queue = await QueueFactory.createQueue('testQueue');
  
  const worker = new Worker(queue, async (job) => {
    console.log('Processing job:', job.id, job.data);
    await new Promise(resolve => setTimeout(resolve, 1000));
    return { processed: true };
  });
  
  worker.on('completed', (jobId, result) => {
    console.log('Job completed:', jobId, result);
  });
  
  console.log('Starting worker...');
  await worker.start();
}

startWorker().catch(console.error);