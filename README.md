To start working with this project all you have to do is next things:

1. Build a docker-compose file
> docker-compose build

2. Start containers
> docker-compose up -d

3. Install All required variables from the WebUI

4. Create a connection with Postgres(dwh)
   - host: dwh
   - user: admin
   - password: admin
   - schema: dwh
   - port: 5432

Now. When all set up you can easily run dags and enjoy them:)
