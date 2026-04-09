WELCOME_EMAIL_TEMPLATE = """
<html>
  <body>
    <h1>Welcome, NAME_HERE!</h1>
    <p>Your account has been created. Your temporary password is: <strong>PASSWORD_HERE</strong></p>
  </body>
</html>
"""

RESET_PASSWORD_EMAIL_TEMPLATE = """
<html>
  <body>
    <h1>Password Reset</h1>
    <p>Your verification code is: <strong>CODE_HERE</strong></p>
    <p>This code expires in 10 minutes.</p>
  </body>
</html>
"""
