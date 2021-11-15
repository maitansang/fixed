import React from "react";
import { Link } from "react-router-dom";

class Page404 extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      submit: false,
    };
  }

  render() {
    return (
      <div>
        <div class="container divMiddle">
          <div class=" text-center col-sm-8 ml-auto mr-auto">
            <div class="col-lg-6 offset-lg-3 col-sm-6 offset-sm-3 col-12 p-3 error-main">
              <div class="row">
                <div class="col-lg-8 col-12 col-sm-10 offset-lg-2 offset-sm-1">
                  <h1 class="m-0">404</h1>
                  <h6>Page not found</h6>
                  <p>
                    <Link to={"/"}>
                      <button type="button" class="btn btn-success">
                        Go Back
                      </button>
                    </Link>
                  </p>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}
export default Page404;
