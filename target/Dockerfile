FROM node:22-alpine
WORKDIR /app
COPY server.js package.json ./
ENV PORT=8080
EXPOSE 8080
USER node
CMD ["node", "server.js"]
