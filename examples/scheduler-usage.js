const QueueFactory = require('../src/QueueFactory');
const Scheduler = require('../src/Scheduler');

async function main() {
  const queue = await QueueFactory.createQueue('myQueue');
  const scheduler = new Scheduler(queue);

  // Add a scheduled job
  const job = await scheduler.addCronJob(
    'dailyReport',
    '* * * * *',  // Run at midnight
    { type: 'report' }
  );

  // Start the scheduler
  await scheduler.start();

  // Manually run the job right now
  await scheduler.runJobById(job.id);

  // Pause the job
  await scheduler.pauseJob(job.id);

  // Resume the job
  await scheduler.resumeJob(job.id);

  // Get all scheduled jobs
  const jobs = await scheduler.getJobs();
  console.log('Scheduled jobs:', jobs);
}

main().catch(console.error);