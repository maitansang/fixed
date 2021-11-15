import React from "react";
import {
  FaUser,
  FaMobile,
  FaAt,
  FaBoxOpen,
  FaGithub,
  FaLinkedin,
  FaLink,
} from "react-icons/fa";

class AboutApp extends React.Component {
  render() {
    return (
      <div>
        <div className="bottom-right">
          <button
            type="button"
            className="btn btn-primary"
            data-toggle="modal"
            data-target="#myModal"
          >
            About
          </button>
        </div>

        <div className="modal" id="myModal">
          <div className="modal-dialog modal-dialog-centered">
            <div className="modal-content">
              <div className="modal-header">
                <h4 className="modal-title">About App</h4>
                <button type="button" className="close" data-dismiss="modal">
                  &times;
                </button>
              </div>

              <div className="modal-body">
                <div id="accordion">
                  <div className="card">
                    <div className="card-header" id="headingOne">
                      <h5 className="mb-0">
                        <button
                          className="btn btn-link"
                          data-toggle="collapse"
                          data-target="#collapseOne"
                          aria-expanded="true"
                          aria-controls="collapseOne"
                        >
                          <FaUser className="mr-3" />
                          About Me
                        </button>
                      </h5>
                    </div>

                    <div
                      id="collapseOne"
                      className="collapse"
                      aria-labelledby="headingOne"
                      data-parent="#accordion"
                    >
                      <div className="card-body">
                        <ul className="list-group">
                          <li className="list-group-item">
                            <FaUser className="mr-3" />
                            Hari Krishna Anem
                            <a
                              href="https://github.com/HariKrishna-9885699666"
                              target="_blank"
                              rel="noopener noreferrer"
                            >
                              <FaGithub className="ml-3" />
                            </a>
                            <a
                              href="https://www.linkedin.com/in/anemharikrishna"
                              target="_blank"
                              rel="noopener noreferrer"
                            >
                              <FaLinkedin className="ml-3" />
                            </a>
                          </li>
                          <li className="list-group-item">
                            <FaMobile className="mr-3" />
                            +91 9885699666
                          </li>
                          <li className="list-group-item">
                            <FaAt className="mr-3" />
                            anemharikrishna@gmail.com
                          </li>
                          <li className="list-group-item">
                            <FaLink className="mr-3" />
                            <a
                              href="https://harikrishna.netlify.app"
                              target="_blank"
                              rel="noopener noreferrer"
                            >
                              Portfolio
                            </a>
                          </li>
                        </ul>
                      </div>
                    </div>
                  </div>

                  <div className="card mt-2">
                    <div className="card-header" id="headingTwo">
                      <h5 className="mb-0">
                        <button
                          className="btn btn-link"
                          data-toggle="collapse"
                          data-target="#collapseTwo"
                          aria-expanded="true"
                          aria-controls="collapseTwo"
                        >
                          <FaBoxOpen className="mr-3" />
                          Packages Used
                        </button>
                      </h5>
                    </div>

                    <div
                      id="collapseTwo"
                      className="collapse"
                      aria-labelledby="headingTwo"
                      data-parent="#accordion"
                    >
                      <div className="card-body">
                        <ul className="list-group">
                          <li className="list-group-item">
                            <FaBoxOpen className="mr-3" />
                            BootStrap
                          </li>
                          <li className="list-group-item">
                            <FaBoxOpen className="mr-3" />
                            Formik
                          </li>
                          <li className="list-group-item">
                            <FaBoxOpen className="mr-3" />
                            Lodash
                          </li>
                          <li className="list-group-item">
                            <FaBoxOpen className="mr-3" />
                            Sweet Alert
                          </li>
                          <li className="list-group-item">
                            <FaBoxOpen className="mr-3" />
                            React Toasts
                          </li>
                          <li className="list-group-item">
                            <FaBoxOpen className="mr-3" />
                            React Native Base 64
                          </li>
                          <li className="list-group-item">
                            <FaBoxOpen className="mr-3" />
                            React Icons
                          </li>
                        </ul>
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              <div className="modal-footer">
                <button
                  type="button"
                  className="btn btn-danger"
                  data-dismiss="modal"
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}
export default AboutApp;
