# 1. LÉPÉS: Építés (Node.js)
FROM node:20-slim as build-stage
WORKDIR /app

# Függőségek telepítése
COPY package*.json ./
RUN npm install

# Forráskód másolása és a Vite build futtatása
COPY . .
RUN npm run build

# 2. LÉPÉS: Kiszolgálás (Nginx)
FROM nginx:stable-alpine as production-stage

# Átmásoljuk a kész 'dist' mappát az Nginx webszerverbe
COPY --from=build-stage /app/dist /usr/share/nginx/html

# Beállítjuk, hogy az Nginx a 80-as porton figyeljen
EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]