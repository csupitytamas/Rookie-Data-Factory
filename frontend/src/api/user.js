import axios from './axios'


export const registerUser = async (email, password) => {
  return await axios.post('/auth/register', {
    email,
    password,
    save_path: "~/Downloads",
    time_zone: "Europe/Budapest",
    preferences: {}
  })
}

export const loginUser = async (email, password, stayLoggedIn = false) => {
  return await axios.post('/auth/login', {
    email,
    password,
    stay_logged_in: stayLoggedIn
  })
}

export const logoutUser = async (token) => {
  return await axios.post('/auth/logout', null, {
    headers: { Authorization: token }
  })
}

export const getCurrentUser = async () => {
  return await axios.get('/users/me')
}
export const getUserByEmail = async (email) => {
  return await axios.get(`/users/${email}`)
}

export const updateUserSettings = async (userId, updates) => {
  return await axios.put(`/users/update/${userId}`, updates)
}