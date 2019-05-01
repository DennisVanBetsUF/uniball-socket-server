FROM node:8-jessie
LABEL maintainer=DennisVanBets

COPY . /home/node
WORKDIR /home/node

RUN npm install
CMD ["node", "src/app.js"]
EXPOSE 9001:9001