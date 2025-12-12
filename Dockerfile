FROM node:18-alpine

# Disable SSL globally for npm and yarn
RUN npm config set strict-ssl false -g

WORKDIR /app

COPY package*.json ./

# Install ALL dependencies for dev mode
RUN yarn config set registry https://registry.npmjs.org/ && \
    yarn config set strict-ssl false && \
    yarn install --network-timeout 600000 --verbose

COPY . .

# Set environment variables
ENV NODE_TLS_REJECT_UNAUTHORIZED=0
ENV NEXT_TELEMETRY_DISABLED=1

EXPOSE 3000
CMD ["yarn", "dev"]