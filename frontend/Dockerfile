# 1. Construction de l'interface React avec Node
FROM node:22-alpine AS construction
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

# 2. Service des fichiers construits par Nginx (léger, pas de Node en production)
FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=construction /app/dist /usr/share/nginx/html
