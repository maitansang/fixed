import React from "react";
import {
  ToastsContainer,
  ToastsStore,
  ToastsContainerPosition,
} from "react-toasts";
import { Redirect, Link } from "react-router-dom";
import base64 from "react-native-base64";
import { Formik } from "formik";
import { validateName, validatePassword } from "../../validations/validations";

class LogIn extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      submit: false,
    };
  }

  render() {
    return (
      <div>
        <Formik
          initialValues={{
            userName: "",
            password: "",
          }}
          validate={(values) => {
            const errors = {};

            errors.userName = validateName(values.userName, "Username") || null;
            errors.password = validatePassword(values.password) || null;

            for (var key in errors) {
              if (errors[key] !== null) return errors;
            }
            return true;
          }}
          onSubmit={(values, actions) => {
            actions.setSubmitting(false);
            let userObj = localStorage.getItem(values.userName);
            if (!userObj) {
              ToastsStore.error("Invalid Username/Password.");
            } else {
              userObj = JSON.parse(userObj);
              const localUname = (userObj && userObj.userName) || null;
              const localUpwd =
                (userObj && base64.decode(userObj.password)) || null;

              if (
                values.userName === localUname &&
                values.password === localUpwd
              ) {
                userObj.isUserLoggedIn = true;
                localStorage.setItem(values.userName, JSON.stringify(userObj));
                this.setState({ submit: true });
              } else {
                ToastsStore.error("Invalid Username/Password.");
              }
            }
          }}
        >
          {(props) => (
            <div className="container mt-2 mb-4 divMiddle">
              <div className="col-sm-8 ml-auto mr-auto">
                <h1 className="display-5 text-center pb-5">
                  <u>React Signup, Signin, Logout features</u>
                </h1>
                <div
                  className="tab-content col-sm-6 ml-auto mr-auto"
                  id="pills-tabContent"
                >
                  <div
                    className="tab-pane fade show active"
                    id="pills-signin"
                    role="tabpanel"
                    aria-labelledby="pills-signin-tab"
                  >
                    <div className="col-sm-12 border border-primary shadow rounded pt-2">
                      <div className="text-center">
                        <img
                          src="./logo.png"
                          className="rounded-circle border p-1 rotate"
                          alt=""
                        />
                      </div>
                      <form onSubmit={props.handleSubmit}>
                        <div className="form-group">
                          <label className="font-weight-bold">
                            Username <span className="text-danger">*</span>
                            <span className="errorMsg">
                              {props.errors.userName &&
                                props.touched.userName &&
                                props.errors.userName}
                            </span>
                          </label>
                          <input
                            type="text"
                            className="form-control"
                            placeholder="UserName"
                            name="userName"
                            onChange={props.handleChange}
                            onBlur={props.handleBlur}
                            value={props.values.userName}
                          />
                        </div>
                        <div className="form-group">
                          <label className="font-weight-bold">
                            Password <span className="text-danger">*</span>
                            <span className="errorMsg">
                              {props.errors.password &&
                                props.touched.password &&
                                props.errors.password}
                            </span>
                          </label>
                          <input
                            type="password"
                            className="form-control"
                            placeholder="***********"
                            name="password"
                            onChange={props.handleChange}
                            onBlur={props.handleBlur}
                            value={props.values.password}
                          />
                        </div>
                        <div className="form-group">
                          <div className="row">
                            <div className="col text-right">
                              {" "}
                              <Link to={"/register"}>Create Account</Link>{" "}
                            </div>
                          </div>
                        </div>
                        <div className="form-group">
                          <input
                            type="submit"
                            name="submit"
                            value="Sign In"
                            className="btn btn-block btn-primary"
                          />
                        </div>
                      </form>
                      {localStorage.getItem(props.values.userName) &&
                      JSON.parse(localStorage.getItem(props.values.userName))
                        .isUserLoggedIn ? (
                        <Redirect
                          to={{
                            pathname: "/dashboard",
                            state: { userName: props.values.userName },
                          }}
                        />
                      ) : null}
                    </div>
                  </div>
                </div>
              </div>
              <ToastsContainer
                store={ToastsStore}
                position={ToastsContainerPosition.TOP_RIGHT}
              />
            </div>
          )}
        </Formik>
      </div>
    );
  }
}
export default LogIn;
