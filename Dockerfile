# Use Node.js 18 on Debian (Bullseye)
FROM node:18-bullseye

# 1. Install Java 11, Maven, and Build Tools
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk maven build-essential python3 && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

# CRITICAL FIX: Symlink for libjvm.so
RUN ln -s /usr/lib/jvm/java-11-openjdk-amd64/lib/server/libjvm.so /usr/lib/libjvm.so

# 2. Set working directory
WORKDIR /app

# 3. Copy dependencies first
COPY package*.json ./

# 4. Install Node Dependencies
RUN npm install --legacy-peer-deps
RUN sed -i 's/BigDecimal/String/g' node_modules/@naxmefy/jdbc/lib/Types.js
# 5. Copy the rest of your application code
COPY . .

# 6. Command to run your app
# UPDATED: Pointing to index.js
CMD ["node", "index.js"]