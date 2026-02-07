FROM node:20-alpine

WORKDIR /app/packages/ingestion

COPY package.json package-lock.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src ./src

RUN npm run build

CMD ["npm", "run", "start"]