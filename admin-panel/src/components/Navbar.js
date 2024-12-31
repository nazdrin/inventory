import React from "react";
import { Link } from "react-router-dom";

const Navbar = ({ authUser }) => {
  return (
    <nav>
      <ul>
        {authUser && (
          <>
            <li>
              <Link to="/developer">Developer Panel</Link>
            </li>
            <li>
              <Link to="/enterprise">Enterprise Panel</Link>
            </li>
          </>
        )}
      </ul>
    </nav>
  );
};

export default Navbar;