import React, { useEffect, useState } from "react";
import { getCurrentUser } from "../services/auth.service";
import { Formik, Field, Form, ErrorMessage } from "formik";
import * as Yup from "yup";
import axios from "axios";

const ExecComponent: React.FC<{value:string,key:string}> = ({children, key,value}) => {
  const currentUser = getCurrentUser();
  const [script, setScript] = useState<string>(value)
  const API_URL = "http://52.116.150.66:8080/api/";

  const initialValues: {
    startDate: string;
    endDate: string;
    ticker: string;
  } = {
    startDate: "",
    endDate: "",
    ticker: "",
  };
  const validationSchema = Yup.object().shape({
    startDate: Yup.string().required("This field is required!"),
    endDate: Yup.string().required("This field is required!"),
  });
  useEffect(() => {
   setScript(value)
  },[value])
  const runScript = (formValue: { startDate: string; endDate: string; ticker : string; }) => {
    const { startDate, endDate,ticker } = formValue;
    var tickerInput = 'all'
    if ((ticker != '') && ticker != null && (ticker != undefined)) {
      tickerInput = ticker
    }
    axios
    .post(API_URL + "script", {
      startDate,
      endDate,
      tickerInput,
      script
    })
    .then((response) => {
      // return response.data;
    });
    console.log("-----123", { startDate, endDate, tickerInput });
  };
  return (
    <div className="container">
      <div className="content-exec">
        <div>{script} script</div>
        <Formik
          initialValues={initialValues}
          validationSchema={validationSchema}
          onSubmit={runScript}
        >
          <Form>
            <div className="form-group">
              <label htmlFor="exampleInputStartDate">Start Date</label>
              <Field
                type="date"
                className="form-control"
                name="startDate"
                id="exampleInputStartDate"
                aria-describedby="startDateHelp"
                placeholder="Enter start date"
              />
               <ErrorMessage
                name="startDate"
                component="div"
                className="alert alert-danger"
              />
              <small
                id="startDateHelp"
                className="form-text text-muted"
              ></small>
            </div>
            <div className="form-group">
              <label htmlFor="exampleInputEndDate">End Date</label>
              <Field
                type="date"
                className="form-control"
                name="endDate"
                id="exampleInputEndDate"
                placeholder="Enter end date"
              />
               <ErrorMessage
                name="endDate"
                component="div"
                className="alert alert-danger"
              />
            </div>
            <div className="form-group">
              <label htmlFor="exampleInputTicker">Ticker</label>
              <Field
                type="text"
                className="form-control"
                id="exampleInputTicker"
                name="ticker"
                placeholder="Enter ticker"
              />
            </div>
            <button type="submit" className="btn btn-primary">
              Run
            </button>
          </Form>
        </Formik>
      </div>
    </div>
  );
};

export default ExecComponent;
