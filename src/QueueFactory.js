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
        
        // Push the schema to the database WITHOUT resetting
        execSync(`npx prisma db push --schema=${schemaPath}`, {
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
                connectionTimeout: 20000,
                queryTimeout: 20000
              }
            }
          });
          
          await prisma.$connect();
          this.prismaClients.set(key, prisma);
        }

        const queue = new Queue(name, {
          ...options,
          prisma: this.prismaClients.get(key)
        });
        
        this.instances.set(key, queue);
      } finally {
        fs.unlinkSync(schemaPath);
      }
    }
    
    return this.instances.get(key);
  }
  
  static createTempSchema(databaseUrl) {
    // Read the original schema file
    const originalSchemaPath = path.join(process.cwd(), 'prisma', 'schema.prisma');
    let schemaContent = fs.readFileSync(originalSchemaPath, 'utf8');
    
    // Replace the database URL in the schema
    schemaContent = schemaContent.replace(
      /url\s*=\s*"[^"]*"/,
      `url = "${databaseUrl}"`
    );
    
    // Create temporary schema file
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
