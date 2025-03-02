const QueueFactory = require('../src/QueueFactory');

async function addJobs() {
  const queue = await QueueFactory.createQueue('myQueue');

  // Repeat every 5 seconds, but only 3 times
  await queue.add('repeatJob', { message: 'Every 5 seconds' }, {
    repeat: {
      every: 5000,
      limit: 3
    }
  });

  // Repeat using cron expression - at midnight (00:00) every day
  await queue.add('cronJob', { message: 'Every day at midnight' }, {
    repeat: {
      cron: '0 0 * * *',  // Changed from '* * * * *' to '0 0 * * *'
      limit: 5
    }
  });

  // Repeat using human-readable interval, limited to 10 executions
  await queue.add('intervalJob', { message: 'Every 2 hours' }, {
    repeat: {
      every: '2 hours',
      limit: 10
    }
  });

  console.log('Jobs added. The cronJob will run at midnight each day.');
}

addJobs().catch(console.error);
