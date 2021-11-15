import React from "react";
import { Route, Redirect } from "react-router-dom";
import _ from "lodash";

function requireAuth(userNameKey) {
  return JSON.parse(localStorage.getItem(userNameKey)).isUserLoggedIn;
}

export const ProtectedRoute = ({ component: Component, ...rest }) => {
  return (
    <Route
      {...rest}
      render={(props) => {
        if (
          _.get(props.location, "state.userName") &&
          requireAuth(props.location.state.userName)
        ) {
          return <Component {...props} />;
        } else {
          return (
            <Redirect
              to={{
                pathname: "/",
              }}
            />
          );
        }
      }}
    />
  );
};
