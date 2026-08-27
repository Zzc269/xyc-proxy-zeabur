FROM denoland/deno:alpine

WORKDIR /app

COPY main.ts deno.json ./

EXPOSE 8080

CMD ["run", "--allow-net", "--allow-env", "main.ts"]
