import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import * as portfinder from 'portfinder';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  try {
    const port = await portfinder.getPortPromise({ port: 3000 });
    await app.listen(port);
    console.log(`Application is running on port ${port}`);
  } catch (error) {
    console.error('Error starting the application:', error);
  }
}

bootstrap();
