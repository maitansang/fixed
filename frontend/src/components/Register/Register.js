import React from "react";
import { Link } from "react-router-dom";
import { Formik } from "formik";
import {
  ToastsContainer,
  ToastsStore,
  ToastsContainerPosition,
} from "react-toasts";
import base64 from "react-native-base64";
import { validateName, validatePassword } from "../../validations/validations";

class Register extends React.Component {
  render() {
    return (
      <div>
        <Formik
          initialValues={{
            firstName: "",
            lastName: "",
            userName: "",
            password: "",
            cpassword: "",
          }}
          validate={(values) => {
            const errors = {};

            errors.firstName =
              validateName(values.firstName, "First Name") || null;
            errors.lastName =
              validateName(values.lastName, "Last Name") || null;
            errors.userName =
              validateName(values.userName, "User Name") || null;
            errors.password =
              validatePassword(values.password, "password") || null;
            errors.cpassword =
              validatePassword(
                values.cpassword,
                "cpassword",
                values.password
              ) || null;

            for (var key in errors) {
              if (errors[key] !== null) return errors;
            }
            return true;
          }}
          onSubmit={(values, actions) => {
            actions.setSubmitting(false);

            if (!localStorage.getItem(values.userName)) {
              localStorage.setItem(
                values.userName,
                JSON.stringify({
                  firstName: values.firstName,
                  lastName: values.lastName,
                  userName: values.userName,
                  password: base64.encode(values.password),
                  isUserLoggedIn: false,
                })
              );

              ToastsStore.success("User registered successfully.");
              actions.resetForm();
            } else {
              ToastsStore.error("Username is already exists.");
            }
          }}
        >
          {(props) => (
            <div className="container mt-2 mb-4 divMiddle">
              <div className="col-sm-4 ml-auto mr-auto">
                <div className="tab-content" id="pills-tabContent">
                  <div
                    className="tab-pane fade show active"
                    id="pills-signup"
                    role="tabpanel"
                    aria-labelledby="pills-signup-tab"
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
                            First Name <span className="text-danger">*</span>
                            <span className="errorMsg">
                              {props.errors.firstName &&
                                props.touched.firstName &&
                                props.errors.firstName}
                            </span>
                          </label>
                          <input
                            type="text"
                            placeholder="Enter First Name"
                            name="firstName"
                            className="form-control"
                            onChange={props.handleChange}
                            onBlur={props.handleBlur}
                            value={props.values.firstName}
                          />
                        </div>
                        <div className="form-group">
                          <label className="font-weight-bold">
                            Las Name <span className="text-danger">*</span>
                            <span className="errorMsg">
                              {props.errors.lastName &&
                                props.touched.lastName &&
                                props.errors.lastName}
                            </span>
                          </label>
                          <input
                            type="text"
                            placeholder="Enter Last Name"
                            name="lastName"
                            className="form-control"
                            onChange={props.handleChange}
                            onBlur={props.handleBlur}
                            value={props.values.lastName}
                          />
                        </div>
                        <div className="form-group">
                          <label className="font-weight-bold">
                            User Name <span className="text-danger">*</span>
                            <span className="errorMsg">
                              {props.errors.userName &&
                                props.touched.userName &&
                                props.errors.userName}
                            </span>
                          </label>
                          <input
                            type="text"
                            placeholder="Enter User Name"
                            name="userName"
                            className="form-control"
                            onChange={props.handleChange}
                            onBlur={props.handleBlur}
                            value={props.values.userName}
                          />
                          <div className="text-danger">
                            <em>This will be your login name!</em>
                          </div>
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
                            name="password"
                            className="form-control"
                            placeholder="***********"
                            onChange={props.handleChange}
                            onBlur={props.handleBlur}
                            value={props.values.password}
                          />
                        </div>
                        <div className="form-group">
                          <label className="font-weight-bold">
                            Confirm Password{" "}
                            <span className="text-danger">*</span>
                            <span className="errorMsg">
                              {props.errors.cpassword &&
                                props.touched.cpassword &&
                                props.errors.cpassword}
                            </span>
                          </label>
                          <input
                            type="password"
                            name="cpassword"
                            className="form-control"
                            placeholder="***********"
                            onChange={props.handleChange}
                            onBlur={props.handleBlur}
                            value={props.values.cpassword}
                          />
                        </div>
                        <div className="form-group">
                          <label>
                            <p>
                              Already have an account?{" "}
                              <Link to={"/"}>Sign In</Link>{" "}
                            </p>
                          </label>
                        </div>
                        <div className="form-group">
                          <input
                            type="submit"
                            name="signupsubmit"
                            value="Sign Up"
                            className="btn btn-block btn-primary"
                          />
                        </div>
                      </form>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )}
        </Formik>
        <ToastsContainer
          store={ToastsStore}
          position={ToastsContainerPosition.TOP_RIGHT}
        />
      </div>
    );
  }
}

export default Register;
