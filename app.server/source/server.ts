
import * as express from "express";

import { handleRequest } from "./capabilities/capabilities";

const PORT = 8090;

let app = express();

app.get(`/`, handleRequest);

app.get('/' , )
app.listen(PORT, () => {
    console.log(`Server listening on: http://localhost:${PORT}`);
});
