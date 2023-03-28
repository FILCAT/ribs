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
import Providers from './routes/Providers';

const router = createBrowserRouter([
    {
        path: "/",
        element: <Root />,
        children: [
            { path: "/", element: <Status /> },
            { path: "groups", element: <Groups /> },
            { path: "providers", element: <Providers /> },
        ],
    },
]);

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(
  <React.StrictMode>
      <RouterProvider router={router} />
  </React.StrictMode>
);
