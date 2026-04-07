FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --production

COPY collector.js db.js query.js ./

CMD ["node", "collector.js"]
