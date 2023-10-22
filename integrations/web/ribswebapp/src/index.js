import React from 'react';
import ReactDOM from 'react-dom/client';
import {
    createBrowserRouter,
    RouterProvider,
} from "react-router-dom";
import './index.css';

import Root from './routes/Root';
import Status from './routes/Status';
import Groups from './routes/Groups';
import Group from "./routes/Group";
import Providers from './routes/Providers';
import Provider from './routes/Provider';
import Content from "./routes/Content";
import Repair from "./routes/Repair";

const router = createBrowserRouter([
    {
        path: "/",
        element: <Root />,
        children: [
            { path: "/", element: <Status /> },
            { path: "groups", element: <Groups /> },
            { path: "groups/:groupKey", element: <Group /> },
            { path: "providers", element: <Providers /> },
            { path: "provider/:providerID", element: <Provider /> },
            { path: "content", element: <Content />},
            { path: "repair", element: <Repair />},
        ],
    },
]);

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(
  <React.StrictMode>
      <RouterProvider router={router} />
  </React.StrictMode>
);
