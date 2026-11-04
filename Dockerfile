FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
# No build step by design - tsx runs the TypeScript directly, same as local
# dev. Each service overrides CMD via docker-compose.
CMD ["npm", "run", "api"]
