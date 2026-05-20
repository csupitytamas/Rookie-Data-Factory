/**
    A fájl az Axios klienst konfigurálja a backenddel való kommunikációhoz.
*/
import axios from 'axios'

// Az alapértelmezett Axios példány létrehozása a backend eléréséhez.
const instance = axios.create({
  baseURL: 'http://localhost:8000',
  withCredentials: true,
})

export default instance