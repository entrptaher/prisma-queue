const { PrismaClient } = require('@prisma/client');
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const Queue = require('./Queue');

class QueueFactory {
  static instances = new Map();
  static prismaClients = new Map();
  
  static async createQueue(name, options = {}) {
    const key = `${name}:${options.databaseUrl || 'default'}`;
    
    if (!this.instances.has(key)) {
      const databaseUrl = options.databaseUrl || 'file:./queue.db';
      
      // Create a temporary prisma schema file for this database instance
      const schemaPath = await this.createTempSchema(databaseUrl);
      
      try {
        // Generate Prisma Client for this schema
        execSync(`npx prisma generate --schema=${schemaPath}`, {
          stdio: 'inherit'
        });
        
        // Push the schema to the database
        execSync(`npx prisma db push --schema=${schemaPath} --accept-data-loss --force-reset`, {
          stdio: 'inherit'
        });

        // Create a new PrismaClient instance if it doesn't exist
        if (!this.prismaClients.has(key)) {
          const prisma = new PrismaClient({
            datasources: {
              db: {
                url: databaseUrl
              }
            },
            __internal: {
              engine: {
                connectionTimeout: 20000, // 20 seconds
                queryTimeout: 20000      // 20 seconds
              }
            }
          });
          
          // Test the connection
          await prisma.$connect();
          
          console.log("DB CONNECTED");

          this.prismaClients.set(key, prisma);
        }

        // Create new queue instance
        const queue = new Queue(name, {
          ...options,
          prisma: this.prismaClients.get(key)
        });
        
        this.instances.set(key, queue);
      } finally {
        // Clean up temp schema file
        fs.unlinkSync(schemaPath);
      }
    }
    
    return this.instances.get(key);
  }
  
  static createTempSchema(databaseUrl) {
    const schemaContent = `
      datasource db {
        provider = "sqlite"
        url      = "${databaseUrl}"
      }
      
      generator client {
        provider = "prisma-client-js"
      }
      
      model Job {
        id          Int           @id @default(autoincrement())
        name        String
        data        String        // JSON stringified data
        status      String        // pending, active, completed, failed, cancelled, waiting, delayed
        priority    Int           @default(0)
        attempts    Int           @default(0)
        maxAttempts Int           @default(3)
        delay       Int           @default(0)
        progress    Float?        // Job progress (0-100)
        processedAt DateTime?
        finishedAt  DateTime?
        createdAt   DateTime      @default(now())
        updatedAt   DateTime      @updatedAt
        result      String?       // JSON stringified result
        error       String?       // Error message if failed
        workerId    String?       // ID of worker processing this job
        stalledAt   DateTime?     // When job was marked as stalled
        cancelledAt DateTime?     // When job was cancelled
        
        // Relations
        dependencies    JobDependency[] @relation("DependentJob")
        dependents     JobDependency[] @relation("DependsOnJob")
        childJobs      JobRelation[]   @relation("ParentJob")
        parentJobs     JobRelation[]   @relation("ChildJob")
      }
      
      model JobDependency {
        id          Int      @id @default(autoincrement())
        jobId       Int
        dependsOnId Int
        job         Job      @relation("DependentJob", fields: [jobId], references: [id], onDelete: Cascade)
        dependsOn   Job      @relation("DependsOnJob", fields: [dependsOnId], references: [id], onDelete: Cascade)
      }
      
      model JobRelation {
        id       Int      @id @default(autoincrement())
        parentId Int
        childId  Int
        parent   Job      @relation("ParentJob", fields: [parentId], references: [id], onDelete: Cascade)
        child    Job      @relation("ChildJob", fields: [childId], references: [id], onDelete: Cascade)
      }
      
      model CronJob {
        id             Int      @id @default(autoincrement())
        jobId         Int
        queueName     String
        jobName       String
        cronExpression String
        data          String   // JSON stringified data
        options       String   // JSON stringified options
        nextRun       DateTime
        createdAt     DateTime @default(now())
        updatedAt     DateTime @updatedAt
      }
    `;
    
    const tempPath = path.join(process.cwd(), `.temp-schema-${Date.now()}.prisma`);
    fs.writeFileSync(tempPath, schemaContent);
    return tempPath;
  }
  
  static async destroyQueue(name, options = {}) {
    const key = `${name}:${options.databaseUrl || 'default'}`;
    const queue = this.instances.get(key);
    
    if (queue) {
      await queue.prisma.$disconnect();
      this.instances.delete(key);
    }
  }
}

module.exports = QueueFactory;
